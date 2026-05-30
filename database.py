# database.py (Production-hardened with defensive logging + Users collection)
import asyncio
import logging
import os
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple, Set

import aiohttp
import motor.motor_asyncio
import pytz
from dateutil.relativedelta import relativedelta
from pymongo import UpdateOne
from pymongo.errors import DuplicateKeyError, OperationFailure

logger = logging.getLogger(__name__)
UTC_TZ = pytz.UTC


def _to_str(value) -> str:
    return str(value) if value is not None else ""

def _safe_int_or_log(value, name="value") -> Optional[int]:
    try:
        return int(str(value).strip())
    except (ValueError, TypeError) as e:
        logger.warning(f"Invalid {name} (expected int): {value!r} - {e}")
        return None

def _ensure_utc(dt):
    if dt is None:
        return None
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
        except ValueError:
            try:
                dt = datetime.fromisoformat(dt)
            except ValueError:
                logger.error(f"Cannot parse datetime string: {dt!r}")
                return None
    if dt.tzinfo is None:
        dt = UTC_TZ.localize(dt)
    else:
        dt = dt.astimezone(UTC_TZ)
    return dt


class LicenseCache:
    # (ဤအပိုင်း မပြောင်းလဲပါ - လိုအပ်ပါက မူလအတိုင်းထားပါ)
    def __init__(self, cleanup_interval: int = 3600):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._cleanup_interval = cleanup_interval
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self.MAX_CACHE_SIZE = 10000

    async def _cleanup_loop(self):
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(self._cleanup_interval)
                if self._stop_event.is_set():
                    break

                now = datetime.now(UTC_TZ)
                async with self._lock:
                    expired_keys = []
                    for uid, data in self._cache.items():
                        cached_at = data.get("cached_at")
                        if cached_at is None:
                            expired_keys.append(uid)
                            continue
                        if (now - cached_at) >= timedelta(seconds=data.get("ttl_seconds", 60)):
                            expired_keys.append(uid)
                    for uid in expired_keys:
                        self._cache.pop(uid, None)

                if expired_keys:
                    logger.debug(f"Cache cleanup: removed {len(expired_keys)} entries")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"LicenseCache cleanup error: {e}")

    def start_cleanup_task(self):
        if self._task is None or self._task.done():
            self._stop_event.clear()
            self._task = asyncio.create_task(self._cleanup_loop())
            logger.info("LicenseCache cleanup task started")

    async def stop_cleanup_task(self):
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
            logger.info("LicenseCache cleanup task stopped")

    async def get(self, user_id: int) -> Optional[Dict[str, Any]]:
        key = str(user_id)
        async with self._lock:
            if key in self._cache:
                data = self._cache[key]
                cached_at = data.get("cached_at")
                ttl = data.get("ttl_seconds", 60)
                if cached_at and (datetime.now(UTC_TZ) - cached_at) < timedelta(seconds=ttl):
                    return data
                del self._cache[key]
            return None

    async def set(self, user_id: int, valid: bool, expiry: Optional[datetime], ttl_seconds: int):
        key = str(user_id)
        async with self._lock:
            if len(self._cache) >= self.MAX_CACHE_SIZE:
                oldest_key = next(iter(self._cache))
                del self._cache[oldest_key]
            self._cache[key] = {
                "valid": valid,
                "expiry": expiry,
                "cached_at": datetime.now(UTC_TZ),
                "ttl_seconds": ttl_seconds
            }

    async def invalidate(self, user_id: int):
        key = str(user_id)
        async with self._lock:
            self._cache.pop(key, None)


class PriceRepository:
    # (မပြောင်းလဲပါ)
    def __init__(self, col):
        self.col = col

    async def setup_indexes(self):
        await self.col.create_index([("type", 1), ("diamond", 1)], unique=True, background=True)
        await self.col.create_index([("is_active", 1), ("type", 1), ("diamond_int", 1)], background=True)
        logger.info("Price indexes ensured.")

    async def add_or_update_price(self, diamond, item_type: str = "dia") -> bool:
        diamond_str = _to_str(diamond).strip()
        if not diamond_str:
            return False
        diamond_int = _safe_int_or_log(diamond_str, "diamond")
        set_fields = {"is_active": True}
        if diamond_int is not None:
            set_fields["diamond_int"] = diamond_int
        try:
            await self.col.update_one(
                {"type": item_type, "diamond": diamond_str},
                {"$set": set_fields, "$setOnInsert": {"monthly_count": 0}},
                upsert=True
            )
            return True
        except DuplicateKeyError:
            logger.error(f"add_or_update_price duplicate diamond: {item_type}/{diamond_str}")
            return False
        except Exception as e:
            logger.error(f"add_or_update_price {item_type}/{diamond_str}: {e}")
            return False

    async def get_active_prices(self, item_type: str = None) -> List[dict]:
        query = {"is_active": True}
        if item_type:
            query["type"] = item_type
        cursor = self.col.find(query).sort("diamond_int", 1).limit(200)
        try:
            return await cursor.to_list(length=200)
        except Exception as e:
            logger.error(f"Error fetching active prices: {e}")
            return []

    async def get_price_by_amount(self, item_type: str, amount) -> Optional[dict]:
        amount_str = _to_str(amount).strip()
        if not amount_str:
            return None
        try:
            return await self.col.find_one({"type": item_type, "diamond": amount_str})
        except Exception as e:
            logger.error(f"Error fetching price {item_type}/{amount_str}: {e}")
            return None

    async def set_price_active(self, item_type: str, amount, is_active: bool):
        amount_str = _to_str(amount).strip()
        if not amount_str:
            return
        await self.col.update_one(
            {"type": item_type, "diamond": amount_str},
            {"$set": {"is_active": is_active}}
        )

    async def get_price_msg_id(self, item_type: str, amount) -> Optional[int]:
        amount_str = _to_str(amount).strip()
        if not amount_str:
            return None
        doc = await self.col.find_one({"type": item_type, "diamond": amount_str}, {"msg_id": 1, "_id": 0})
        return doc.get("msg_id") if doc else None

    async def set_price_msg_id(self, item_type: str, amount, msg_id: int):
        amount_str = _to_str(amount).strip()
        if not amount_str:
            return
        result = await self.col.update_one(
            {"type": item_type, "diamond": amount_str},
            {"$set": {"msg_id": msg_id}}
        )
        if result.matched_count == 0:
            logger.warning(f"set_price_msg_id: no price doc for {item_type}/{amount_str}")

    async def increment_monthly_count(self, quantity, item_type: str = "dia"):
        quantity_str = _to_str(quantity).strip()
        if not quantity_str:
            return
        result = await self.col.update_one(
            {"type": item_type, "diamond": quantity_str},
            {"$inc": {"monthly_count": 1}}
        )
        if result.matched_count == 0:
            logger.warning(f"Price not found for increment: {item_type}/{quantity_str}")

    async def get_all_with_monthly_counts(self) -> List[dict]:
        cursor = self.col.find({"monthly_count": {"$gt": 0}}).sort("diamond_int", 1)
        return await cursor.to_list(length=500)

    async def reset_monthly_counts_bulk(self, items: List[Tuple[str, str]]):
        if not items:
            return
        ops = [
            UpdateOne({"type": t, "diamond": d}, {"$set": {"monthly_count": 0}})
            for t, d in items
        ]
        try:
            res = await self.col.bulk_write(ops, ordered=False)
            logger.info(f"Reset monthly counts for {len(items)} items, modified={res.modified_count}")
        except Exception as e:
            logger.error(f"Bulk reset monthly counts failed: {e}")

    async def migrate_legacy_diamond_types(self) -> int:
        updated = 0
        ops = []
        try:
            cursor = self.col.find({"diamond": {"$not": {"$type": "string"}}})
            async for doc in cursor:
                diamond = doc.get("diamond")
                new_val = str(diamond) if diamond is not None else ""
                diamond_int = _safe_int_or_log(new_val, "diamond")
                set_fields = {"diamond": new_val}
                if diamond_int is not None:
                    set_fields["diamond_int"] = diamond_int
                ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": set_fields}))
                if len(ops) >= 500:
                    res = await self.col.bulk_write(ops, ordered=False)
                    updated += res.modified_count
                    ops = []
            if ops:
                res = await self.col.bulk_write(ops, ordered=False)
                updated += res.modified_count
            logger.info(f"Migrated {updated} non‑string diamonds.")
            return updated
        except DuplicateKeyError as e:
            logger.error(f"Bulk migration DuplicateKeyError: {e}")
            return -1
        except Exception as e:
            logger.error(f"Legacy migration error: {e}")
            return -1


class OrderRepository:
    # (မပြောင်းလဲပါ)
    def __init__(self, col):
        self.col = col

    async def setup_indexes(self):
        await self.col.create_index("order_id", unique=True, background=True)
        await self.col.create_index("user_id", background=True)
        await self.col.create_index([("status", 1), ("expire_at", 1)], background=True)
        logger.info("Order indexes ensured.")

    async def create_order(self, order_id: str, user_id, profile_name: str,
                           dia, price_snapshot, item_type: str,
                           region: str = "Myanmar") -> Optional[dict]:
        user_id_int = _safe_int_or_log(user_id, f"create_order user_id for {order_id}")
        if user_id_int is None:
            logger.error("Invalid user_id, aborting.")
            return None
        now = datetime.now(UTC_TZ)
        expire_at = now + timedelta(minutes=5)
        order_data = {
            "order_id": order_id,
            "user_id": user_id_int,
            "profile_name": profile_name,
            "order_info": {
                "quantity": _to_str(dia),
                "price_snapshot": _to_str(price_snapshot),
                "game_id": None,
                "zone_id": None,
                "item_type": item_type,
                "region": region
            },
            "status": "pending_id",
            "timestamps": {
                "created_at": now,
                "updated_at": now
            },
            "expire_at": expire_at
        }
        try:
            await self.col.insert_one(order_data)
            return order_data
        except DuplicateKeyError:
            logger.error(f"Duplicate order_id {order_id} – already exists.")
            return None
        except Exception as e:
            logger.error(f"Failed to create order {order_id}: {e}")
            return None

    async def get_timeout_orders(self, limit: int = 50) -> list:
        now = datetime.now(UTC_TZ)
        cursor = self.col.find({
            "status": {"$in": ["pending_id", "waiting_payment", "confirming_id"]},
            "expire_at": {"$lt": now}
        }).limit(limit)
        return await cursor.to_list(length=limit)

    async def get_total_users_count(self) -> int:
        try:
            pipe = [{"$group": {"_id": "$user_id"}}, {"$count": "total"}]
            result = await self.col.aggregate(pipe).to_list(length=1)
            return result[0]["total"] if result else 0
        except Exception as e:
            logger.error(f"Error counting total users: {e}")
            return 0

    async def update_order_game_id(self, order_id: str, game_id: str, zone_id: str):
        now = datetime.now(UTC_TZ)
        expire_at = now + timedelta(minutes=5)
        await self.col.update_one(
            {"order_id": order_id},
            {"$set": {
                "order_info.game_id": game_id,
                "order_info.zone_id": zone_id,
                "status": "confirming_id",
                "timestamps.updated_at": now,
                "expire_at": expire_at
            }}
        )

    async def update_order_status(self, order_id: str, new_status: str):
        now = datetime.now(UTC_TZ)
        set_fields = {
            "status": new_status,
            "timestamps.updated_at": now
        }
        update_doc = {"$set": set_fields}

        if new_status == "waiting_payment":
            set_fields["expire_at"] = now + timedelta(minutes=15)
        elif new_status in ("completed", "failed", "cancelled", "timeout"):
            update_doc["$unset"] = {"expire_at": ""}

        await self.col.update_one({"order_id": order_id}, update_doc)

    async def get_order(self, order_id: str):
        try:
            return await self.col.find_one({"order_id": order_id})
        except Exception as e:
            logger.error(f"Error fetching order {order_id}: {e}")
            return None

    async def set_order_admin_msg_id(self, order_id: str, admin_msg_id: int):
        await self.col.update_one(
            {"order_id": order_id},
            {"$set": {"admin_msg_id": admin_msg_id}}
        )

    async def delete_order(self, order_id: str):
        await self.col.delete_one({"order_id": order_id})

    async def generate_monthly_report_data(self, year_month: str) -> dict:
        try:
            start = datetime.strptime(f"{year_month}-01", "%Y-%m-%d")
            start = UTC_TZ.localize(start)
            end = start + relativedelta(months=1)
            pipe = [
                {"$match": {"status": "completed",
                            "timestamps.created_at": {"$gte": start, "$lt": end}}},
                {"$count": "total_orders"}
            ]
            result = await self.col.aggregate(pipe).to_list(length=1)
            total = result[0]["total_orders"] if result else 0
            return {"total_orders": total}
        except Exception as e:
            logger.error(f"Monthly report error for {year_month}: {e}")
            return {"total_orders": 0}

    async def purge_old_orders(self, days: int = 90) -> int:
        cutoff = datetime.now(UTC_TZ) - timedelta(days=days)
        res = await self.col.delete_many({"timestamps.created_at": {"$lt": cutoff}})
        return res.deleted_count


class LicenseRepository:
    # (ဤအပိုင်းတွင် add_or_update ကိုသာ ပြင်ဆင်ထားပါသည်)
    def __init__(self, col, cache: LicenseCache,
                 http_session: Optional[aiohttp.ClientSession],
                 master_api_url: str, secret_token: str, is_client_mode: bool):
        self.col = col
        self.cache = cache
        self.master_url = master_api_url
        self.secret = secret_token
        self.client_mode = is_client_mode
        self._session = http_session
        self._own_session = False
        self._pending: Dict[int, asyncio.Task] = {}
        self._pending_lock = asyncio.Lock()

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
            self._own_session = True
            logger.debug("Created new internal aiohttp session.")

    async def _check_local(self, user_id: int) -> Tuple[bool, Optional[datetime]]:
        try:
            doc = await self.col.find_one({"user_id": user_id})
        except Exception as e:
            logger.exception(f"DB error checking license for {user_id}")
            return False, None
        if not doc:
            return False, None
        expiry = _ensure_utc(doc.get("expiry_date"))
        if expiry is None:
            return False, None
        return expiry > datetime.now(UTC_TZ), expiry

    async def check_license_local(self, user_id: int) -> Tuple[bool, Optional[datetime]]:
        return await self._check_local(user_id)

    async def _fetch_from_master(self, user_id: int) -> Tuple[bool, Optional[datetime]]:
        if not self.master_url:
            raise ValueError("MASTER_API_URL not set")
        await self._ensure_session()
        url = f"{self.master_url.rstrip('/')}/api/license/check/{user_id}"
        timeout = aiohttp.ClientTimeout(total=10)
        async with self._session.get(url, timeout=timeout,
                                     headers={"Authorization": f"Bearer {self.secret}"}) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise Exception(f"Master API returned {resp.status}: {text}")
            data = await resp.json()
            return data.get("valid", False), _ensure_utc(data.get("expiry"))

    async def _fetch_fresh(self, user_id: int) -> Tuple[bool, Optional[datetime]]:
        if self.client_mode:
            try:
                valid_master, expiry_master = await self._fetch_from_master(user_id)
                if valid_master and expiry_master:
                    await self.col.update_one(
                        {"user_id": user_id},
                        {"$set": {"expiry_date": expiry_master}},
                        upsert=True
                    )
                    return True, expiry_master
                else:
                    await self.col.delete_one({"user_id": user_id})
                    return False, None
            except Exception as e:
                logger.warning(f"Master fetch failed for {user_id}: {e}")
                cached = await self.cache.get(user_id)
                if cached:
                    return cached.get("valid", False), cached.get("expiry")
                return False, None
        return await self._check_local(user_id)

    async def is_license_valid(self, user_id: int) -> bool:
        user_id = _safe_int_or_log(user_id, "is_license_valid")
        if user_id is None:
            return False

        cached = await self.cache.get(user_id)
        if cached is not None:
            expiry = cached.get("expiry")
            if cached.get("valid") is True and expiry and expiry > datetime.now(UTC_TZ):
                return True
            if cached.get("valid") is False:
                return False

        async with self._pending_lock:
            if user_id in self._pending:
                task = self._pending[user_id]
            else:
                try:
                    task = asyncio.create_task(self._fetch_and_cache(user_id))
                    def _safe_cleanup(uid):
                        async def _clean():
                            async with self._pending_lock:
                                self._pending.pop(uid, None)
                        asyncio.create_task(_clean())
                    task.add_done_callback(lambda _: _safe_cleanup(user_id))
                    self._pending[user_id] = task
                except Exception:
                    logger.exception(f"Failed to create fetch task for {user_id}")
                    return False

        try:
            return await task
        except Exception as e:
            logger.exception(f"License task failed for {user_id}")
            return False

    async def _fetch_and_cache(self, user_id: int) -> bool:
        try:
            valid, expiry = await self._fetch_fresh(user_id)
            ttl = 86400 if valid else 60
            await self.cache.set(user_id, valid, expiry, ttl)
            return valid
        except Exception as e:
            logger.exception(f"Fresh license fetch failed for {user_id}")
            cached = await self.cache.get(user_id)
            if cached and cached.get("expiry") and cached["expiry"] > datetime.now(UTC_TZ):
                return True
            return False
        finally:
            async with self._pending_lock:
                self._pending.pop(user_id, None)

    async def force_refresh(self, user_id: int) -> bool:
        user_id = _safe_int_or_log(user_id, "force_refresh")
        if user_id is None:
            return False
        await self.cache.invalidate(user_id)
        return await self.is_license_valid(user_id)

    async def background_refresh(self, user_id: int):
        user_id = _safe_int_or_log(user_id, "background_refresh")
        if user_id is None:
            return
        try:
            valid, expiry = await self._fetch_fresh(user_id)
        except Exception as e:
            logger.warning(f"Background refresh failed for {user_id}: {e}")
            return
        ttl = 86400 if valid else 60
        await self.cache.set(user_id, valid, expiry, ttl)

    async def add_or_update(self, user_id, months: int) -> bool:
        user_id = _safe_int_or_log(user_id, "add_license user_id")
        months = _safe_int_or_log(months, "months")
        if user_id is None or months is None:
            return False
        try:
            now = datetime.now(UTC_TZ)
            doc = await self.col.find_one({"user_id": user_id})
            if doc and doc.get("expiry_date"):
                current = _ensure_utc(doc["expiry_date"])
                current = current or now
                base = current if current > now else now
            else:
                base = now
            new_expiry = base + relativedelta(months=months)
            await self.col.update_one(
                {"user_id": user_id},
                {"$set": {"expiry_date": new_expiry}},
                upsert=True
            )
            await self.cache.invalidate(user_id)
            logger.info(f"License updated for {user_id}: +{months} month(s)")
            return True
        except Exception as e:
            logger.exception(f"Error updating license for {user_id}")
            # 🛠 FIX: raise မလုပ်တော့ဘဲ return False ပြန်ပါ
            return False

    async def get_all_licenses(self):
        cursor = self.col.find({}).limit(1000)
        return await cursor.to_list(length=1000)

    async def cleanup_expired(self) -> int:
        now = datetime.now(UTC_TZ)
        expired_ids = []
        cursor = self.col.find({"expiry_date": {"$lt": now}}, {"user_id": 1})
        async for doc in cursor:
            expired_ids.append(doc["user_id"])
        if not expired_ids:
            return 0
        res = await self.col.delete_many({"user_id": {"$in": expired_ids}})
        deleted = res.deleted_count
        for uid in expired_ids:
            await self.cache.invalidate(uid)
        logger.info(f"Cleaned up {deleted} expired licenses")
        return deleted

    async def revoke_license(self, user_id: int) -> bool:
        user_id = _safe_int_or_log(user_id, "revoke_license")
        if user_id is None:
            return False
        try:
            await self.col.delete_one({"user_id": user_id})
            await self.cache.invalidate(user_id)
            logger.info(f"License revoked for {user_id}")
            return True
        except Exception as e:
            logger.exception(f"Error revoking license for {user_id}")
            return False

    async def close(self):
        # ✅ Pending tasks များကို cancel လုပ်ပြီး ရှင်းထုတ်ခြင်း
        async with self._pending_lock:
            for task in self._pending.values():
                task.cancel()
            self._pending.clear()
        if self._own_session and self._session:
            await self._session.close()
            self._session = None
            logger.debug("Internal aiohttp session closed.")


class BannedUserRepository:
    # (မပြောင်းလဲပါ)
    def __init__(self, col, banned_set: Set[int]):
        self.col = col
        self.banned_set = banned_set

    async def setup_indexes(self):
        await self.col.create_index("user_id", unique=True, background=True)

    async def load_banned_users_from_db(self):
        self.banned_set.clear()
        async for doc in self.col.find({}):
            try:
                self.banned_set.add(int(doc["user_id"]))
            except (ValueError, TypeError):
                continue
        logger.info(f"Loaded {len(self.banned_set)} banned users from DB")

    async def ban(self, user_id: int) -> bool:
        user_id = _safe_int_or_log(user_id, "ban_user")
        if user_id is None:
            return False
        try:
            await self.col.update_one(
                {"user_id": user_id},
                {"$set": {"banned_at": datetime.now(UTC_TZ)}},
                upsert=True
            )
            self.banned_set.add(user_id)
            return True
        except Exception as e:
            logger.exception(f"Error banning {user_id}")
            return False

    async def unban(self, user_id: int) -> bool:
        user_id = _safe_int_or_log(user_id, "unban_user")
        if user_id is None:
            return False
        try:
            res = await self.col.delete_one({"user_id": user_id})
            self.banned_set.discard(user_id)
            return res.deleted_count > 0
        except Exception as e:
            logger.exception(f"Error unbanning {user_id}")
            return False

    async def is_banned(self, user_id: int) -> bool:
        try:
            return int(user_id) in self.banned_set
        except (ValueError, TypeError):
            return False


class SettingsRepository:
    # (မပြောင်းလဲပါ)
    def __init__(self, col):
        self.col = col

    async def setup_indexes(self):
        try:
            await self.col.delete_many({"config_key": None})
        except Exception as e:
            logger.warning(f"Failed to clean null config_key documents: {e}")
        await self.col.create_index("config_key", unique=True, sparse=True, background=True)
        await self.col.create_index("setting_type", background=True)
        logger.info("Settings indexes ensured (sparse unique on config_key).")

    async def set_service_status(self, is_open: bool):
        status = "Open" if is_open else "Stop"
        await self.col.update_one(
            {"setting_type": "maintenance"},
            {"$set": {"status": status}},
            upsert=True
        )

    async def get_service_status(self) -> bool:
        doc = await self.col.find_one({"setting_type": "maintenance"})
        return False if doc and doc.get("status") == "Stop" else True

    async def set_config(self, key: str, value):
        await self.col.update_one(
            {"config_key": key},
            {"$set": {"config_value": value}},
            upsert=True
        )

    async def get_config(self, key: str):
        doc = await self.col.find_one({"config_key": key})
        return doc["config_value"] if doc else None

    async def set_bot_info(self, username: str, link: str = None) -> bool:
        if not link and username:
            link = f"https://t.me/{username.lstrip('@')}"
        try:
            await self.col.update_one(
                {"setting_type": "bot_info"},
                {"$set": {"bot_username": username, "bot_link": link}},
                upsert=True
            )
            return True
        except Exception as e:
            logger.exception("Error setting bot info")
            return False

    async def get_bot_info(self) -> dict:
        doc = await self.col.find_one({"setting_type": "bot_info"})
        if doc:
            return {"bot_username": doc.get("bot_username", ""), "bot_link": doc.get("bot_link", "")}
        return {"bot_username": "", "bot_link": ""}


class MonthlyReportRepository:
    # (မပြောင်းလဲပါ)
    def __init__(self, col):
        self.col = col

    async def setup_indexes(self):
        await self.col.create_index("report_date", expireAfterSeconds=7776000, background=True)

    async def upsert_report(self, month: str, data: dict):
        await self.col.update_one(
            {"month": month},
            {"$set": {"data": data, "report_date": datetime.now(UTC_TZ)}},
            upsert=True
        )

    async def purge_old(self, cutoff_month: str) -> int:
        res = await self.col.delete_many({"month": {"$lt": cutoff_month}})
        return res.deleted_count


class UsersRepository:
    """Lightweight user store for broadcast (Render‑friendly)."""

    def __init__(self, col):
        self.col = col

    async def setup_indexes(self):
        # 🛠 FIX: user_id အပြင် last_seen အတွက် index ထည့်ပါ
        await self.col.create_index("user_id", unique=True, background=True)
        await self.col.create_index("last_seen", background=True)

    async def upsert_user(self, user_id: int):
        # 🛠 FIX: last_seen timestamp ထည့်ပါ
        try:
            await self.col.update_one(
                {"user_id": user_id},
                {"$set": {
                    "user_id": user_id,
                    "last_seen": datetime.now(UTC_TZ)
                }},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Failed to upsert user {user_id}: {e}")

    async def get_all_user_ids(self, limit: int = 10000) -> List[int]:
        cursor = self.col.find({}, {"user_id": 1, "_id": 0}).limit(limit)
        ids = []
        async for doc in cursor:
            ids.append(doc["user_id"])
        return ids

    # 🛠 FIX: method အသစ် – Ban မဟုတ်သောသူများ ရေတွက်
    async def get_active_user_count(self, banned_set: set) -> int:
        try:
            query = {"user_id": {"$nin": list(banned_set)}} if banned_set else {}
            return await self.col.count_documents(query)
        except Exception as e:
            logger.error(f"get_active_user_count failed: {e}")
            return 0

    # 🛠 FIX: method အသစ် – Blocked user ဖယ်ရှား
    async def remove_user(self, user_id: int):
        try:
            await self.col.delete_one({"user_id": user_id})
        except Exception as e:
            logger.error(f"remove_user {user_id} failed: {e}")


class DatabaseManager:
    def __init__(self, uri: str, db_name: str = "DiamondBotDB",
                 http_session: Optional[aiohttp.ClientSession] = None,
                 banned_set: Optional[Set[int]] = None,
                 primary_admin_id: Optional[int] = None):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(
            uri,
            connect=False,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            socketTimeoutMS=20000,
            retryWrites=True,
            retryReads=True,
            maxPoolSize=10,
            minPoolSize=0,
            waitQueueTimeoutMS=5000,
            maxIdleTimeMS=60000,
        )
        self.db = self.client[db_name]
        self._http_session = http_session
        self._banned_set = banned_set if banned_set is not None else set()
        self._primary_admin_id = primary_admin_id

        self.price_repo      = PriceRepository(self.db['Prices'])
        self.order_repo      = OrderRepository(self.db['Orders'])
        self.license_cache   = LicenseCache()
        master_url = os.getenv("MASTER_API_URL", "").strip()
        secret     = os.getenv("API_SECRET_TOKEN", "").strip()
        client_mode = bool(master_url)
        self.license_repo    = LicenseRepository(self.db['Licenses'], self.license_cache,
                                                 self._http_session, master_url, secret, client_mode)
        self.banned_repo     = BannedUserRepository(self.db['BannedUsers'], self._banned_set)
        self.settings_repo   = SettingsRepository(self.db['Settings'])
        self.report_repo     = MonthlyReportRepository(self.db['Monthly_Reports'])
        self.users_repo      = UsersRepository(self.db['Users'])

        self._cache_cleanup_started = False

    @property
    def prices(self): return self.db['Prices']
    @property
    def orders(self): return self.db['Orders']
    @property
    def monthly_reports(self): return self.db['Monthly_Reports']
    @property
    def settings(self): return self.db['Settings']
    @property
    def licenses(self): return self.db['Licenses']
    @property
    def banned_users(self): return self.db['BannedUsers']
    @property
    def nickname_cache(self): return self.db['nickname_cache']
    @property
    def users(self): return self.db['Users']
    @property
    def primary_admin_id(self): return self._primary_admin_id

    def is_super_admin(self, user_id: int) -> bool:
        return self._primary_admin_id is not None and user_id == self._primary_admin_id

    async def setup_indexes(self):
        try:
            await self.price_repo.setup_indexes()
            await self.order_repo.setup_indexes()
            await self.license_repo.col.create_index("user_id", unique=True, background=True)
            await self.report_repo.setup_indexes()
            await self.banned_repo.setup_indexes()
            await self.settings_repo.setup_indexes()
            await self.users_repo.setup_indexes()
            # ✅ Nickname cache အတွက် compound unique index ထည့်ခြင်း
            await self.db['nickname_cache'].create_index(
                [("game_id", 1), ("zone_id", 1)],
                unique=True,
                background=True
            )
            await self.db['nickname_cache'].create_index(
                "timestamp", expireAfterSeconds=86400, background=True
            )
            logger.info("✅ All indexes verified/created.")
        except Exception as e:
            logger.exception("❌ Failed to setup some indexes, continuing anyway.")

    async def ping(self) -> bool:
        try:
            await self.client.admin.command('ping')
            return True
        except Exception as e:
            logger.exception("❌ MongoDB ping failed")
            return False

    def start_cache_cleanup(self):
        if not self._cache_cleanup_started:
            self.license_cache.start_cleanup_task()
            self._cache_cleanup_started = True

    async def close(self):
        await self.license_cache.stop_cleanup_task()
        await self.license_repo.close()
        self.client.close()
        logger.info("DatabaseManager closed.")

    async def generate_monthly_report(self, year_month: str) -> dict:
        report_data = await self.order_repo.generate_monthly_report_data(year_month)
        prices = await self.price_repo.get_all_with_monthly_counts()
        items_sold = []
        items_to_reset = []
        for p in prices:
            item_type_str = "Dia" if p.get("type", "dia") == "dia" else "UC"
            diamond = p.get("diamond", "?")
            count = p.get("monthly_count", 0)
            items_sold.append(f"{diamond} {item_type_str} ({count})")
            ptype = p.get("type")
            pdiamond = p.get("diamond")
            if ptype and pdiamond:
                items_to_reset.append((ptype, pdiamond))
        if items_to_reset:
            await self.price_repo.reset_monthly_counts_bulk(items_to_reset)
        items_sold_str = ", ".join(items_sold) if items_sold else "အရောင်းမရှိပါ"
        report = {"Total Orders": report_data["total_orders"], "Items Sold": items_sold_str}
        await self.report_repo.upsert_report(year_month, report)
        return report

    async def purge_3_months_old_data(self) -> Tuple[int, int]:
        del_orders = await self.order_repo.purge_old_orders(90)
        three_months_ago = datetime.now(UTC_TZ) - timedelta(days=90)
        cutoff_month = three_months_ago.strftime("%Y-%m")
        del_reports = await self.report_repo.purge_old(cutoff_month)
        return del_orders, del_reports

    async def migrate_legacy_diamond_types(self) -> int:
        return await self.price_repo.migrate_legacy_diamond_types()
