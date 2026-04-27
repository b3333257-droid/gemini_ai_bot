# database.py - Purge 3 months only (No TTL index, no immediate cleanup)
import motor.motor_asyncio
from datetime import datetime, timedelta
import pytz
import asyncio
import logging
import os
import aiohttp
from typing import Optional, Tuple, Dict, Any

logger = logging.getLogger(__name__)

UTC_TZ = pytz.UTC

import master


class LicenseCache:
    """
    လိုင်စင်စစ်ဆေးမှုများကို မှတ်ဉာဏ်ထဲတွင် ခေတ္တသိမ်းဆည်းရန် Cache စနစ်။
    TTL နှစ်မျိုးသုံးနိုင်သည် - valid တွေ့လျှင် 24h၊ invalid တွေ့လျှင် 5min
    """
    def __init__(self, cleanup_interval_seconds: int = 3600):
        self.cache: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._cleanup_interval = cleanup_interval_seconds
        self._cleanup_task: Optional[asyncio.Task] = None
        self._stop_cleanup = False

    async def _cleanup_expired(self):
        while not self._stop_cleanup:
            try:
                await asyncio.sleep(self._cleanup_interval)
                if self._stop_cleanup:
                    break
                async with self._lock:
                    now = datetime.now(UTC_TZ)
                    expired_users = []
                    for user_id, data in self.cache.items():
                        cached_at = data.get("cached_at")
                        ttl = data.get("ttl_seconds", 60)
                        if cached_at and now - cached_at >= timedelta(seconds=ttl):
                            expired_users.append(user_id)
                    # Batch delete expired entries
                    for user_id in expired_users:
                        del self.cache[user_id]
                    if expired_users:
                        logger.debug(f"Cache cleanup: removed {len(expired_users)} expired entries")
            except asyncio.CancelledError:
                logger.info("LicenseCache cleanup task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in LicenseCache cleanup task: {e}")

    def start_cleanup_task(self):
        # ✅ Restart-safe fix: recreate task if it was previously done or cancelled
        if self._cleanup_task is None or self._cleanup_task.done():
            self._stop_cleanup = False
            self._cleanup_task = asyncio.create_task(self._cleanup_expired())
            logger.info("LicenseCache cleanup task started")

    async def stop_cleanup_task(self):
        self._stop_cleanup = True
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
            logger.info("LicenseCache cleanup task stopped")

    async def get(self, user_id: int) -> Optional[Dict[str, Any]]:
        user_id = str(user_id)
        async with self._lock:
            if user_id in self.cache:
                data = self.cache[user_id]
                cached_at = data.get("cached_at")
                ttl = data.get("ttl_seconds", 60)
                if datetime.now(UTC_TZ) - cached_at < timedelta(seconds=ttl):
                    return data
                else:
                    del self.cache[user_id]
            return None

    async def set(self, user_id: int, valid: bool, expiry_date: Optional[datetime], ttl_seconds: int):
        user_id = str(user_id)
        async with self._lock:
            self.cache[user_id] = {
                "valid": valid,
                "expiry": expiry_date,
                "cached_at": datetime.now(UTC_TZ),
                "ttl_seconds": ttl_seconds
            }

    async def invalidate(self, user_id: int):
        user_id = str(user_id)
        async with self._lock:
            if user_id in self.cache:
                del self.cache[user_id]


class DatabaseManager:
    def __init__(self, uri: str, db_name: str = "DiamondBotDB"):
        try:
            self.client = motor.motor_asyncio.AsyncIOMotorClient(
                uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=20000,
                retryWrites=True,
                retryReads=True,
                maxPoolSize=50,
                minPoolSize=5,
                waitQueueTimeoutMS=5000,
                maxIdleTimeMS=60000,
            )
            self.db = self.client[db_name]
            logger.info("AsyncIOMotorClient initialized with connection pool settings.")
        except Exception as e:
            logger.critical(f"Failed to initialize MongoDB client: {e}")
            raise

        self.prices = self.db['Prices']
        self.orders = self.db['Orders']
        self.monthly_reports = self.db['Monthly_Reports']
        self.settings = self.db['Settings']
        self.licenses = self.db['Licenses']
        self.banned_users = self.db['BannedUsers']  # ✅ Banned Users Collection

        self.license_cache = LicenseCache()
        self._cache_cleanup_started = False

        self.MASTER_API_URL = os.getenv("MASTER_API_URL", "").strip()
        self.API_SECRET_TOKEN = os.getenv("API_SECRET_TOKEN", "").strip()
        self.ADMIN_ID = None

        self.CACHE_TTL_VALID = 24 * 60 * 60      # 24 hours
        self.CACHE_TTL_INVALID = 5 * 60          # 5 minutes

        self.is_client_mode = bool(self.MASTER_API_URL)
        if self.is_client_mode:
            logger.info(f"Running in CLIENT mode. Master API: {self.MASTER_API_URL}")
        else:
            logger.info("Running in MASTER mode (no MASTER_API_URL set).")

        # ---------- Fix 1: AIOHTTP Session Management ----------
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_created_at: Optional[datetime] = None

    def set_admin_id(self, admin_id: int):
        self.ADMIN_ID = admin_id

    def start_cache_cleanup(self):
        if not self._cache_cleanup_started:
            self.license_cache.start_cleanup_task()
            self._cache_cleanup_started = True

    # ---------- Fix 1: Session getter with auto-refresh every 24h ----------
    async def _get_session(self) -> aiohttp.ClientSession:
        """Return a reusable ClientSession, recreate if older than 24 hours."""
        now = datetime.now(UTC_TZ)
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
            self._session_created_at = now
            logger.debug("Created new aiohttp ClientSession")
        elif self._session_created_at and (now - self._session_created_at) > timedelta(hours=24):
            # Close old session and create new one
            await self._session.close()
            self._session = aiohttp.ClientSession()
            self._session_created_at = now
            logger.debug("Refreshed aiohttp ClientSession (older than 24h)")
        return self._session

    async def close(self):
        """Close database connection and aiohttp session."""
        try:
            await self.license_cache.stop_cleanup_task()
        except Exception as e:
            logger.error(f"Error stopping license cache cleanup: {e}")

        # Close persistent aiohttp session if exists
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info("Closed aiohttp ClientSession")

        try:
            self.client.close()
            logger.info("MongoDB client closed successfully.")
        except Exception as e:
            logger.error(f"Error closing MongoDB client: {e}")

    # ----------------------------------------------------------------------
    # Indexing & Connection
    # ----------------------------------------------------------------------
    async def setup_indexes(self):
        try:
            # Regular indexes (ရှာဖွေမှုမြန်ဆန်ရန်)
            await self.orders.create_index("order_id", unique=True, background=True)
            await self.orders.create_index("user_id", background=True)

            # TTL index ကို ဖြုတ်ထားပါသည် (data ကို ၃ လထားရန် ရည်ရွယ်ချက်ဖြင့်)
            # purge_3_months_old_data() ကို အသုံးပြုမည်

            await self.orders.create_index(
                [("status", 1), ("timestamps.updated_at", 1)],
                background=True
            )
            await self.prices.create_index(
                [("type", 1), ("diamond", 1)],
                unique=True,
                background=True
            )
            await self.licenses.create_index("user_id", unique=True, background=True)
            await self.monthly_reports.create_index(
                "report_date",
                expireAfterSeconds=7776000,  # 90 days
                background=True,
                name="report_ttl_90d"
            )

            # Banned Users index
            await self.banned_users.create_index("user_id", unique=True, background=True)

            logger.info("✅ Database indexes created/verified successfully.")
        except Exception as e:
            logger.error(f"❌ Index setup failed: {e}")
            raise

    async def ping(self) -> bool:
        try:
            await self.client.admin.command('ping')
            logger.info("✅ MongoDB connection successful.")
            return True
        except Exception as e:
            logger.error(f"❌ MongoDB ping failed: {e}")
            return False

    # ----------------------------------------------------------------------
    # Order Management
    # ----------------------------------------------------------------------
    async def create_order(self, order_id: str, user_id: int, profile_name: str,
                           dia: str, price_snapshot: int, item_type: str) -> Optional[dict]:
        now = datetime.now(UTC_TZ)
        order_data = {
            "order_id": order_id,
            "user_id": user_id,                     # int
            "profile_name": profile_name,
            "order_info": {
                "quantity": dia,                    # quantity key
                "price_snapshot": str(price_snapshot),
                "game_id": None,
                "zone_id": None,
                "item_type": item_type
            },
            "status": "pending_id",
            "timestamps": {
                "created_at": now,                  # purging အတွက် လိုအပ်
                "updated_at": now
            }
        }
        try:
            await self.orders.insert_one(order_data)
            return order_data
        except Exception as e:
            logger.error(f"Failed to create order {order_id}: {e}")
            return None

    async def increment_monthly_count(self, quantity: str, item_type: str = "dia"):
        try:
            result = await self.prices.update_one(
                {"type": item_type, "diamond": quantity},
                {"$inc": {"monthly_count": 1}}
            )
            if result.matched_count == 0:
                logger.warning(f"Price not found for increment: type={item_type}, diamond={quantity}")
        except Exception as e:
            logger.error(f"Error incrementing monthly count for {item_type}/{quantity}: {e}")

    async def get_timeout_orders(self, limit: int = 50) -> list:
        now = datetime.now(UTC_TZ)
        pending_limit = now - timedelta(minutes=5)
        payment_limit = now - timedelta(minutes=15)

        cursor_pending = self.orders.find(
            {"status": "pending_id", "timestamps.updated_at": {"$lt": pending_limit}}
        ).limit(limit)
        cursor_waiting = self.orders.find(
            {"status": "waiting_payment", "timestamps.updated_at": {"$lt": payment_limit}}
        ).limit(limit)

        try:
            pending_orders = await cursor_pending.to_list(length=limit)
            waiting_orders = await cursor_waiting.to_list(length=limit)
            orders = pending_orders + waiting_orders
            if len(orders) > limit:
                orders = orders[:limit]
            return orders
        except Exception as e:
            logger.error(f"Error fetching timeout orders: {e}")
            return []

    async def get_total_users_count(self) -> int:
        try:
            pipeline = [
                {"$group": {"_id": "$user_id"}},
                {"$count": "total"}
            ]
            result = await self.orders.aggregate(pipeline).to_list(length=1)
            return result[0]["total"] if result else 0
        except Exception as e:
            logger.error(f"Error counting total users: {e}")
            return 0

    # ----------------------------------------------------------------------
    # License Checking (Core)
    # ----------------------------------------------------------------------
    async def check_license_local(self, user_id: int) -> Tuple[bool, Optional[datetime]]:
        try:
            doc = await self.licenses.find_one({"user_id": user_id})   # int
        except Exception as e:
            logger.error(f"DB error checking license for {user_id}: {e}")
            return False, None

        if not doc:
            return False, None

        now_utc = datetime.now(UTC_TZ)
        expiry = doc["expiry_date"]
        if expiry.tzinfo is None:
            expiry = UTC_TZ.localize(expiry)

        return expiry > now_utc, expiry

    async def fetch_license_from_master(self, user_id: int) -> Tuple[bool, Optional[datetime]]:
        if not self.MASTER_API_URL:
            raise ValueError("MASTER_API_URL is not configured")

        base_url = self.MASTER_API_URL.rstrip('/')
        url = f"{base_url}/api/license/check/{user_id}?secret={self.API_SECRET_TOKEN}"

        timeout = aiohttp.ClientTimeout(total=10)
        session = await self._get_session()
        try:
            async with session.get(url, timeout=timeout) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"Master API returned status {resp.status}: {text}")
                data = await resp.json()
                valid = data.get("valid", False)
                expiry_str = data.get("expiry")
                expiry = None
                if expiry_str:
                    try:
                        expiry = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
                        if expiry.tzinfo is None:
                            expiry = UTC_TZ.localize(expiry)
                    except Exception:
                        pass
                return valid, expiry
        except asyncio.TimeoutError:
            raise Exception("Timeout connecting to Master API")
        except aiohttp.ClientError as e:
            raise Exception(f"HTTP error connecting to Master API: {e}")

    async def _get_license_fresh(self, user_id: int) -> Tuple[bool, Optional[datetime]]:
        if self.is_client_mode:
            return await self.fetch_license_from_master(user_id)
        else:
            return await self.check_license_local(user_id)

    async def is_license_valid(self, user_id: int) -> bool:
        if master.MASTER_ID is not None and str(user_id) == str(master.MASTER_ID):
            return True

        cached = await self.license_cache.get(user_id)
        if cached is not None:
            expiry = cached.get("expiry")
            if expiry is not None:
                if expiry > datetime.now(UTC_TZ):
                    return True
                else:
                    pass
            else:
                if not cached.get("valid", False):
                    return False

        try:
            valid, expiry = await self._get_license_fresh(user_id)
        except Exception as e:
            logger.error(f"Failed to fetch fresh license for {user_id}: {e}")
            if cached is not None and cached.get("expiry") is not None:
                return cached["expiry"] > datetime.now(UTC_TZ)
            return False

        if valid:
            await self.license_cache.set(user_id, True, expiry, self.CACHE_TTL_VALID)
            return True
        else:
            await self.license_cache.set(user_id, False, None, self.CACHE_TTL_INVALID)
            return False

    async def force_refresh_license(self, user_id: int) -> bool:
        try:
            valid, expiry = await self._get_license_fresh(user_id)
        except Exception as e:
            logger.error(f"Force refresh failed for {user_id}: {e}")
            cached = await self.license_cache.get(user_id)
            if cached is not None and cached.get("expiry") is not None:
                return cached["expiry"] > datetime.now(UTC_TZ)
            return False

        if valid:
            await self.license_cache.set(user_id, True, expiry, self.CACHE_TTL_VALID)
        else:
            await self.license_cache.set(user_id, False, None, self.CACHE_TTL_INVALID)
        return valid

    async def background_refresh_license(self, user_id: int):
        try:
            valid, expiry = await self._get_license_fresh(user_id)
        except Exception as e:
            logger.warning(f"Background refresh failed for {user_id}: {e}")
            return

        if valid:
            await self.license_cache.set(user_id, True, expiry, self.CACHE_TTL_VALID)
        else:
            await self.license_cache.set(user_id, False, None, self.CACHE_TTL_INVALID)

    # ----------------------------------------------------------------------
    # License Management
    # ----------------------------------------------------------------------
    async def add_or_update_license(self, user_id: int, months: int):
        try:
            doc = await self.licenses.find_one({"user_id": user_id})   # int
            now_utc = datetime.now(UTC_TZ)
            days_to_add = months * 30

            if doc and doc.get("expiry_date"):
                current_expiry = doc["expiry_date"]
                if current_expiry.tzinfo is None:
                    current_expiry = UTC_TZ.localize(current_expiry)

                if current_expiry > now_utc:
                    new_expiry = current_expiry + timedelta(days=days_to_add)
                else:
                    new_expiry = now_utc + timedelta(days=days_to_add)
            else:
                new_expiry = now_utc + timedelta(days=days_to_add)

            await self.licenses.update_one(
                {"user_id": user_id},              # int
                {"$set": {"expiry_date": new_expiry}},
                upsert=True
            )
            await self.license_cache.invalidate(user_id)
            logger.info(f"License updated for user {user_id}: +{months} month(s), new expiry: {new_expiry}")
            return True
        except Exception as e:
            logger.error(f"Error updating license for {user_id}: {e}")
            raise

    # ----------------------------------------------------------------------
    # Order Helpers
    # ----------------------------------------------------------------------
    async def update_order_game_id(self, order_id: str, game_id: str, zone_id: str):
        try:
            await self.orders.update_one(
                {"order_id": order_id},
                {
                    "$set": {
                        "order_info.game_id": game_id,
                        "order_info.zone_id": zone_id,
                        "status": "confirming_id",
                        "timestamps.updated_at": datetime.now(UTC_TZ)
                    }
                }
            )
        except Exception as e:
            logger.error(f"Error updating game_id for order {order_id}: {e}")

    async def update_order_status(self, order_id: str, new_status: str):
        try:
            await self.orders.update_one(
                {"order_id": order_id},
                {
                    "$set": {
                        "status": new_status,
                        "timestamps.updated_at": datetime.now(UTC_TZ)
                    }
                }
            )
        except Exception as e:
            logger.error(f"Error updating status for order {order_id}: {e}")

    async def get_order(self, order_id: str):
        try:
            return await self.orders.find_one({"order_id": order_id})
        except Exception as e:
            logger.error(f"Error fetching order {order_id}: {e}")
            return None

    async def set_order_admin_msg_id(self, order_id: str, admin_msg_id: int):
        try:
            await self.orders.update_one(
                {"order_id": order_id},
                {"$set": {"admin_msg_id": admin_msg_id}}
            )
        except Exception as e:
            logger.error(f"Error setting admin_msg_id for {order_id}: {e}")

    async def delete_order(self, order_id: str):
        try:
            await self.orders.delete_one({"order_id": order_id})
        except Exception as e:
            logger.error(f"Error deleting order {order_id}: {e}")

    # ----------------------------------------------------------------------
    # Price Management
    # ----------------------------------------------------------------------
    async def add_or_update_price(self, diamond: str, item_type: str = "dia") -> bool:
        try:
            await self.prices.update_one(
                {"type": item_type, "diamond": diamond},
                {
                    "$set": {"is_active": True},
                    "$setOnInsert": {"monthly_count": 0}
                },
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error updating price {item_type}/{diamond}: {e}")
            return False

    async def get_active_prices(self, item_type: str = None):
        query = {"is_active": True}
        if item_type:
            query["type"] = item_type
        cursor = self.prices.find(query).sort("diamond", 1).limit(200)
        try:
            return await cursor.to_list(length=200)
        except Exception as e:
            logger.error(f"Error fetching active prices: {e}")
            return []

    async def get_price_by_amount(self, item_type: str, amount):
        try:
            return await self.prices.find_one({"type": item_type, "diamond": str(amount)})
        except Exception as e:
            logger.error(f"Error fetching price {item_type}/{amount}: {e}")
            return None

    async def set_price_active(self, item_type: str, amount: str, is_active: bool):
        try:
            await self.prices.update_one(
                {"type": item_type, "diamond": amount},
                {"$set": {"is_active": is_active}}
            )
        except Exception as e:
            logger.error(f"Error setting price active {item_type}/{amount}: {e}")

    async def get_price_msg_id(self, item_type: str, amount: str):
        doc = await self.get_price_by_amount(item_type, amount)
        return doc.get("msg_id") if doc else None

    async def set_price_msg_id(self, item_type: str, amount: str, msg_id: int):
        try:
            await self.prices.update_one(
                {"type": item_type, "diamond": amount},
                {"$set": {"msg_id": msg_id}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error setting price msg_id for {item_type}/{amount}: {e}")

    # ----------------------------------------------------------------------
    # Settings & Config
    # ----------------------------------------------------------------------
    async def set_service_status(self, is_open: bool):
        status_str = "Open" if is_open else "Stop"
        try:
            await self.settings.update_one(
                {"setting_type": "maintenance"},
                {"$set": {"status": status_str}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error setting service status: {e}")

    async def get_service_status(self) -> bool:
        try:
            setting = await self.settings.find_one({"setting_type": "maintenance"})
            if setting and setting.get("status") == "Stop":
                return False
            return True
        except Exception as e:
            logger.error(f"Error getting service status: {e}")
            return True

    async def set_config(self, key: str, value):
        try:
            await self.settings.update_one(
                {"config_key": key},
                {"$set": {"config_value": value}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error setting config {key}: {e}")

    async def get_config(self, key: str):
        try:
            doc = await self.settings.find_one({"config_key": key})
            return doc.get("config_value") if doc else None
        except Exception as e:
            logger.error(f"Error getting config {key}: {e}")
            return None

    async def set_bot_info(self, username: str, link: str = None) -> bool:
        if not link and username:
            link = f"https://t.me/{username.lstrip('@')}"
        try:
            await self.settings.update_one(
                {"setting_type": "bot_info"},
                {"$set": {"bot_username": username, "bot_link": link}},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error setting bot info: {e}")
            return False

    async def get_bot_info(self) -> dict:
        try:
            doc = await self.settings.find_one({"setting_type": "bot_info"})
            if doc:
                return {"bot_username": doc.get("bot_username", ""), "bot_link": doc.get("bot_link", "")}
        except Exception as e:
            logger.error(f"Error getting bot info: {e}")
        return {"bot_username": "", "bot_link": ""}

    async def get_all_licenses(self):
        try:
            cursor = self.licenses.find({}).limit(1000)
            return await cursor.to_list(length=1000)
        except Exception as e:
            logger.error(f"Error getting all licenses: {e}")
            return []

    # ----------------------------------------------------------------------
    # Banned Users Management (CRUD) - with real-time memory sync
    # ----------------------------------------------------------------------
    async def ban_user(self, user_id: int) -> bool:
        try:
            result = await self.banned_users.update_one(
                {"user_id": user_id},               # int
                {"$set": {"banned_at": datetime.now(UTC_TZ)}},
                upsert=True
            )
            # Sync to memory set
            master.BANNED_USERS.add(user_id)
            return result.upserted_id is not None or result.modified_count > 0
        except Exception as e:
            logger.error(f"Error banning user {user_id}: {e}")
            return False

    async def unban_user(self, user_id: int) -> bool:
        try:
            result = await self.banned_users.delete_one({"user_id": user_id})   # int
            # Sync to memory set (remove if present)
            master.BANNED_USERS.discard(user_id)
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error unbanning user {user_id}: {e}")
            return False

    async def is_user_banned(self, user_id: int) -> bool:
        # Directly check in-memory set (assumes loaded at startup and synced)
        return user_id in master.BANNED_USERS

    # ----------------------------------------------------------------------
    # License Cleanup (Batch Optimization)
    # ----------------------------------------------------------------------
    async def cleanup_expired_licenses(self) -> int:
        try:
            now = datetime.now(UTC_TZ)
            result = await self.licenses.delete_many({"expiry_date": {"$lt": now}})
            deleted = result.deleted_count
            if deleted > 0:
                logger.info(f"Cleaned up {deleted} expired licenses from database.")
            return deleted
        except Exception as e:
            logger.error(f"Error cleaning up expired licenses: {e}")
            return 0

    # ----------------------------------------------------------------------
    # Monthly Report & 3-Month Purge
    # ----------------------------------------------------------------------
    async def generate_monthly_report(self, year_month: str):
        try:
            start_date = datetime.strptime(f"{year_month}-01", "%Y-%m-%d")
            start_date = UTC_TZ.localize(start_date)
            if start_date.month == 12:
                end_date = datetime(start_date.year + 1, 1, 1, tzinfo=UTC_TZ)
            else:
                end_date = datetime(start_date.year, start_date.month + 1, 1, tzinfo=UTC_TZ)

            pipeline = [
                {"$match": {"status": "completed", "timestamps.created_at": {"$gte": start_date, "$lt": end_date}}},
                {"$count": "total_orders"}
            ]
            result = await self.orders.aggregate(pipeline).to_list(length=1)
            total_orders = result[0]["total_orders"] if result else 0

            prices_cursor = self.prices.find({"monthly_count": {"$gt": 0}}).sort("diamond", 1).limit(200)
            prices_data = await prices_cursor.to_list(length=200)

            items_sold_list = []
            items_to_reset = []
            for p in prices_data:
                item_type_str = "Dia" if p.get("type", "dia") == "dia" else "UC"
                items_sold_list.append(f"{p['diamond']} {item_type_str} ({p['monthly_count']})")
                items_to_reset.append((p["type"], p["diamond"]))
            items_sold_str = ", ".join(items_sold_list) if items_sold_list else "အရောင်းမရှိပါ"

            report_data = {"Total Orders": total_orders, "Items Sold": items_sold_str}
            await self.monthly_reports.update_one(
                {"month": year_month},
                {"$set": {"data": report_data, "report_date": datetime.now(UTC_TZ)}},
                upsert=True
            )
            for item_type, diamond in items_to_reset:
                await self.prices.update_one(
                    {"type": item_type, "diamond": diamond},
                    {"$set": {"monthly_count": 0}}
                )
            return report_data
        except Exception as e:
            logger.error(f"Error generating monthly report for {year_month}: {e}")
            return {"Total Orders": 0, "Items Sold": "Error"}

    async def purge_3_months_old_data(self) -> Tuple[int, int]:
        """
        ၃ လ (ရက် ၉၀) ကျော်သွားသော အော်ဒါများနှင့် လစဉ်အစီရင်ခံစာများကို ဖျက်သည်။
        ဤ function ကို နေ့စဉ် (သို့) လစဉ် schedule အဖြစ် ခေါ်ပေးရန် အကြံပြုပါသည်။
        """
        try:
            three_months_ago = datetime.now(UTC_TZ) - timedelta(days=90)
            del_orders = await self.orders.delete_many({"timestamps.created_at": {"$lt": three_months_ago}})
            target_month_str = three_months_ago.strftime("%Y-%m")
            del_reports = await self.monthly_reports.delete_many({"month": {"$lt": target_month_str}})
            return del_orders.deleted_count, del_reports.deleted_count
        except Exception as e:
            logger.error(f"Error purging old data: {e}")
            return 0, 0
