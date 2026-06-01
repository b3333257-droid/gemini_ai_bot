# cacu.py
import ast
import operator
import re

# ==============================
# Calculator Logic
# ==============================

# ခွင့်ပြုထားသော operator များ (unary +,- ပါဝင်)
ALLOWED_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}

def _safe_eval_node(node):
    """AST node တစ်ခုချင်းစီကို safe စနစ်ဖြင့် တွက်ချက်"""
    if isinstance(node, ast.Constant):  # Python 3.8+ (3.11 နဲ့ ကိုက်ညီ)
        return node.value
    elif isinstance(node, ast.BinOp):
        left = _safe_eval_node(node.left)
        right = _safe_eval_node(node.right)
        op_type = type(node.op)
        if op_type not in ALLOWED_OPS:
            raise ValueError(f"ခွင့်မပြုသော operator: {op_type}")
        if op_type == ast.Div and right == 0:
            raise ZeroDivisionError("သုညနဲ့ စား၍မရပါ")
        return ALLOWED_OPS[op_type](left, right)
    elif isinstance(node, ast.UnaryOp):
        operand = _safe_eval_node(node.operand)
        op_type = type(node.op)
        if op_type not in ALLOWED_OPS:
            raise ValueError(f"ခွင့်မပြုသော unary operator: {op_type}")
        return ALLOWED_OPS[op_type](operand)
    else:
        raise ValueError("သင်္ချာ အသုံးအနှုန်းမဟုတ်ပါ")

def safe_calculate(expr: str):
    """
    Myanmar/Unicode operator တွေကို standard ပြောင်း၊ ရာခိုင်နှုန်း handle လုပ်၊
    AST နဲ့ safe evaluate လုပ်။ Error ရှိရင် None ပြန်။
    """
    # Convert Myanmar operators to Python operators
    expr = expr.replace("×", "*").replace("÷", "/").replace("^", "**")
    # Remove spaces
    expr = expr.replace(" ", "")

    # % → /100 (ဥပမာ 50% → 50/100, -50% → (-50/100), .5% → (.5/100))
    expr = re.sub(r'([+-]?(?:\d+\.?\d*|\.\d+))%', r'(\1/100)', expr)

    if not expr:
        return None

    try:
        tree = ast.parse(expr, mode='eval')
        result = _safe_eval_node(tree.body)

        # Float result ကို သန့်ရှင်းအောင်ပြုလုပ်
        if isinstance(result, float):
            # .0 ဖြစ်နေရင် integer ပြန်
            if result == int(result):
                return int(result)
            # Decimal 10 နေရာအထိ လျှော့
            return round(result, 10)
        return result
    except (SyntaxError, ValueError, ZeroDivisionError, OverflowError):
        # သင်္ချာဆိုင်ရာ error မှန်သမျှကို None ပြန်
        return None

def is_math_expression(text: str):
    """
    Message က calculator နဲ့တွက်ရမယ့် expression လား စစ်ဆေး။
    - digit, operator, parentheses, space, ^ တွေပဲပါရမယ်
    - အနည်းဆုံး operator တစ်ခုပါရမယ် (unary negative လည်းပါဝင်)
    - အရှည် ၅၀ လုံးထက်မပိုပါစေနဲ့
    """
    if not text or len(text) > 50:
        return False

    # ခွင့်ပြုထားသော characters: ဂဏန်း, operator, ., (), space, ×, ÷, ^
    if re.search(r'[^\d+\-*/%.() ×÷\s^]', text):
        return False

    # အနည်းဆုံး ဂဏန်းတစ်လုံး ပါရမည်
    if not re.search(r'\d', text):
        return False

    # အနည်းဆုံး operator တစ်ခုပါရမည် (+,-,*,/,×,÷,%,^)
    if not re.search(r'[+\-*/×÷%^]', text):
        return False

    return True

def format_result(original: str, result) -> str:
    """မူရင်း expression နဲ့ ရလဒ်ကို UI format ပြန်ထုတ်"""
    return f"🧮 {original}\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n= {result}"
