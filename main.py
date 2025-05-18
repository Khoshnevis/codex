import asyncio, logging, os, re
from contextlib import suppress

import aiohttp
import telegram.error                         # ← NEW
from dotenv import load_dotenv
from slugify import slugify
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ContextTypes, MessageHandler, filters
)

import db
from db import history_diff

load_dotenv()

BOT_TOKEN      = os.getenv("BOT_TOKEN")
PORT           = int(os.getenv("PORT", "8081"))
WEBHOOK_URL    = os.getenv("WEBHOOK_URL", "").rstrip("/")
INITIAL_ADMIN  = int(os.getenv("ADMIN_TELEGRAM_ID", "0") or 0)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s | %(message)s")
logger = logging.getLogger(__name__)

re_sig  = re.compile(r"signals?\/(\d+)", re.I)
re_url  = re.compile(r"https?://\S+", re.I)
re_name = re.compile(r"^([^|]+)\|(.+)$", re.S)

# ---------- keyboards ----------
def main_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📈 Manage Signals", callback_data="manage_sig")],
        [InlineKeyboardButton("👥 Manage Users",   callback_data="manage_usr")],
    ])

def sig_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add",  callback_data="sig_add")],
        [InlineKeyboardButton("➖ Rem",  callback_data="sig_del")],
        [InlineKeyboardButton("📜 List",callback_data="sig_list")],
        [InlineKeyboardButton("⬅️ Back",callback_data="back")],
    ])

def usr_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add",  callback_data="usr_add")],
        [InlineKeyboardButton("➖ Rem",  callback_data="usr_del")],
        [InlineKeyboardButton("⭐ Toggle admin", callback_data="usr_toggle")],
        [InlineKeyboardButton("📜 List",callback_data="usr_list")],
        [InlineKeyboardButton("⬅️ Back",callback_data="back")],
    ])

# ---------- helpers ----------
async def ensure_root():
    if INITIAL_ADMIN:
        await db.add_user(INITIAL_ADMIN, name="root", admin=True)

async def url_ok(url: str) -> bool:
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
            with suppress(Exception):
                async with s.head(url, allow_redirects=True) as r:
                    return r.status == 200
            async with s.get(url, allow_redirects=True) as r:
                return r.status == 200
    except Exception:
        return False

# ---------- handlers ----------
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await db.is_admin(update.effective_user.id):
        await update.message.reply_text("⛔️ Not authorised.")
        return
    await update.message.reply_text("Welcome!", reply_markup=main_kb())

async def menu_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    # ---- tolerate stale callbacks ----
    try:
        await q.answer()
    except telegram.error.BadRequest as e:
        if "query is too old" not in str(e).lower():
            raise
    d = q.data

    if not await db.is_admin(q.from_user.id):
        await q.edit_message_text("⛔️ Unauthorized.")
        return

    # ----- Signals submenu -----
    if d == "manage_sig":
        await q.edit_message_text("Signal menu:", reply_markup=sig_kb())

    elif d == "sig_list":
        rows = await db.list_signals()
        text = ("📜 *Signals*:\n" +
                "\n".join(f"{r['id']} → {r['url']}" for r in rows)) if rows else "ℹ️ None."
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=sig_kb())

    # ----- Users submenu -----
    elif d == "manage_usr":
        await q.edit_message_text("User menu:", reply_markup=usr_kb())

    elif d == "usr_list":
        rows = await db.list_users()
        if rows:
            lines = [f"{'⭐' if r['admin'] else '▫️'} `{r['id']}` – *{slugify(r['name'] or '-') }* :: {r['desc'] or '-'}"
                     for r in rows]
            text = "📜 *Users*:\n" + "\n".join(lines)
        else:
            text = "ℹ️ No users."
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=usr_kb())

    # ----- Navigation -----
    elif d in ("back", "back_main"):          # ← accepts legacy data
        await q.edit_message_text("Menu:", reply_markup=main_kb())

    # ----- Awaiting text -----
    elif d in ("sig_add", "sig_del", "usr_add", "usr_del", "usr_toggle"):
        ctx.user_data["await"] = d
        prompt = {
            "sig_add": "Send full signal URL.",
            "sig_del": "Send signal ID to remove.",
            "usr_add": "Send: <telegram_id>|<name or note>",
            "usr_del": "Send user ID to remove.",
            "usr_toggle": "Send user ID to promote/demote."
        }[d]
        await q.edit_message_text(
            prompt,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Cancel", callback_data="back")]])
        )

async def text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    act = ctx.user_data.pop("await", None)
    txt = update.message.text.strip()
    me  = update.effective_user

    if not await db.is_admin(me.id):
        await update.message.reply_text("⛔️ Not authorised.")
        return
    if not act:
        await update.message.reply_text("Use the buttons.")
        return

    # ---------- Signals ----------
    if act == "sig_add":
        murl = re_url.search(txt)
        if not murl:
            return await update.message.reply_text("No URL.")
        url = murl.group(0)
        mid = re_sig.search(url)
        if not mid:
            return await update.message.reply_text("Bad signal link.")
        sid = mid.group(1)
        if await db.signal_exists(sid):
            return await update.message.reply_text("Already stored.")
        await update.message.reply_text("Checking URL…")
        if not await url_ok(url):
            return await update.message.reply_text("URL dead.")
        await db.add_signal(sid, url)
        await update.message.reply_text("Added.", reply_markup=main_kb())

    elif act == "sig_del":
        sid = re.sub(r"\D", "", txt)
        await db.remove_signal(sid)
        await update.message.reply_text("Removed.", reply_markup=main_kb())

    # ---------- Users ----------
    elif act == "usr_add":
        m = re_name.match(txt)
        if not m:
            return await update.message.reply_text("Use `<id>|<name>` format.")
        uid_str, note = m.group(1).strip(), m.group(2).strip()
        if not uid_str.isdigit():
            return await update.message.reply_text("ID must be digits.")
        uid = int(uid_str)
        ok = await db.add_user(uid, name=note.split()[0], desc=note)
        await update.message.reply_text("Added." if ok else "Exists.", reply_markup=main_kb())

    elif act == "usr_del":
        uid_str = re.sub(r"\D", "", txt)
        if not uid_str:
            return await update.message.reply_text("Need ID.")
        if int(uid_str) == me.id:
            return await update.message.reply_text("Can't remove yourself.")
        await db.remove_user(int(uid_str))
        await update.message.reply_text("Removed.", reply_markup=main_kb())

    elif act == "usr_toggle":
        uid_str = re.sub(r"\D", "", txt)
        if not uid_str:
            return await update.message.reply_text("Need ID.")
        uid = int(uid_str)
        new_state = not await db.is_admin(uid)
        await db.set_admin(uid, new_state)
        await update.message.reply_text(
            "⭐ Promoted." if new_state else "⬇ Demoted.",
            reply_markup=main_kb(),
        )

async def stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await db.is_admin(update.effective_user.id):
        await update.message.reply_text("⛔")
        return
    if not ctx.args:
        await update.message.reply_text("Usage: /stats <signal_id>")
        return
    sid = ctx.args[0]
    info = await history_diff(sid)
    if not info:
        await update.message.reply_text("No data.")
        return
    cur = info["current"]
    diff = info.get("diff") or {}
    def fmt(label, key):
        val = cur.get(key)
        d = diff.get(key)
        if d is not None and d != 0:
            sign = "+" if d > 0 else ""
            return f"{label}: {val} ({sign}{d})"
        return f"{label}: {val}"
    lines = [
        f"📊 *{cur.get('name') or sid}*",
        fmt("Growth", "growth"),
        fmt("Drawdown", "drawdown"),
        fmt("Monthly", "monthly_growth"),
        fmt("Weeks", "weeks"),
        fmt("Trades", "trades"),
        fmt("Profit", "profit_trades"),
        fmt("Loss", "loss_trades"),
    ]
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

# ---------- bootstrap ----------
if __name__ == "__main__":
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN missing")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(db.init())
    loop.run_until_complete(ensure_root())

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CallbackQueryHandler(menu_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text))

    app.run_webhook(
        listen="0.0.0.0", port=PORT, url_path="telegram",
        webhook_url=f"{WEBHOOK_URL}/telegram",
        drop_pending_updates=True
    )

