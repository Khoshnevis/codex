import asyncio
import logging
import os
import re
import random
from contextlib import suppress

import aiohttp
import telegram.error
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.helpers import escape_markdown
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ContextTypes, MessageHandler, filters,
)

import db
import scraper

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "8081"))
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").rstrip("/")
INITIAL_ADMIN = int(os.getenv("ADMIN_TELEGRAM_ID", "0") or 0)
SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL", "3600"))
APP = None

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s | %(message)s")
logger = logging.getLogger(__name__)

re_sig = re.compile(r"signals?/(\d+)", re.I)
re_url = re.compile(r"https?://\S+", re.I)
re_name = re.compile(r"^([^|]+)\|(.+)$", re.S)

# Escape special characters for Telegram MarkdownV2
def md(text: str) -> str:
    return escape_markdown(str(text), version=2)

# ---------- keyboards ----------
def main_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📈 Manage Signals", callback_data="manage_sig")],
        [InlineKeyboardButton("👥 Manage Users", callback_data="manage_usr")],
    ])

def sig_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add", callback_data="sig_add")],
        [InlineKeyboardButton("➖ Rem", callback_data="sig_del")],
        [InlineKeyboardButton("📜 List", callback_data="sig_list")],
        [InlineKeyboardButton("📊 Stats", callback_data="sig_stats")],
        [InlineKeyboardButton("⬅ Back", callback_data="back")],
    ])

def stats_kb(rows):
    kb = [
        [InlineKeyboardButton(f"{r['name']} ({r['id']})" if r.get('name') else r['id'], callback_data=f"stat_{r['id']}")] for r in rows
    ]
    kb.append([InlineKeyboardButton("⬅ Back", callback_data="manage_sig")])
    return InlineKeyboardMarkup(kb)

def usr_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add", callback_data="usr_add")],
        [InlineKeyboardButton("➖ Rem", callback_data="usr_del")],
        [InlineKeyboardButton("⭐ Toggle admin", callback_data="usr_toggle")],
        [InlineKeyboardButton("📜 List", callback_data="usr_list")],
        [InlineKeyboardButton("⬅ Back", callback_data="back")],
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

async def scrape_all():
    rows = await db.list_signals()
    for idx, r in enumerate(rows):
        try:
            data = await scraper.scrape(r["url"])
            await db.add_history(r["id"], **data)
            await db.update_signal_info(r["id"], name=data.get("name"), weeks=data.get("weeks"), latest_trade=data.get("latest_trade"), start_year=data.get("start_year"))
            info = await db.history_diff(r["id"])
            if info and info.get("diff"):
                changes = []
                for k, dv in info["diff"].items():
                    if dv:
                        changes.append(f"{k}: {dv:+}")
                if changes:
                    text = f"\u2139 Updates for {info['latest']['name']} ({r['id']}):\n" + "\n".join(changes)
                    uids = await db.list_user_ids()
                    for uid in uids:
                        try:
                            await APP.bot.send_message(uid, text)
                        except Exception:
                            pass
        except Exception as e:
            logger.exception("scrape %s failed: %s", r["id"], e)
        if idx != len(rows) - 1:
            await asyncio.sleep(random.uniform(5, 15))

async def periodic_scrape():
    while True:
        await scrape_all()
        await asyncio.sleep(SCRAPE_INTERVAL)

# ---------- handlers ----------
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await db.is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Unauthorized")
        return
    await update.message.reply_text("Welcome!", reply_markup=main_kb())

async def menu_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    try:
        await q.answer()
    except telegram.error.BadRequest as e:
        if "query is too old" not in str(e).lower():
            raise
    d = q.data

    if not await db.is_admin(q.from_user.id):
        await q.edit_message_text("⛔ Unauthorized")
        return

    if d == "manage_sig":
        await q.edit_message_text("Signal menu:", reply_markup=sig_kb())

    elif d == "sig_list":
        rows = await db.list_signals()
        if rows:
            lines = [f"{r['id']} → {md(r['url'])}" for r in rows]
            text = "📜 *Signals*:\n" + "\n".join(lines)
        else:
            text = "ℹ None"
        await q.edit_message_text(text, parse_mode="MarkdownV2", reply_markup=sig_kb())

    elif d == "sig_stats":
        rows = await db.list_signals()
        if not rows:
            await q.edit_message_text("ℹ None", reply_markup=sig_kb())
        else:
            await q.edit_message_text(
                "Select signal:",
                reply_markup=stats_kb(rows),
            )

    elif d.startswith("stat_"):
        sid = d.split("_", 1)[1]
        info = await db.history_diff(sid)
        if not info:
            await q.edit_message_text("No history.", reply_markup=sig_kb())
        else:
            latest = info["latest"]
            diff = info["diff"]
            lines = [f"*{md(latest['name'])}* \({sid}\)"]
            for k in [
                "growth",
                "drawdown",
                "monthly_growth",
                "weeks",
                "trades",
                "profit_trades",
                "loss_trades",
                "start_year",
                "latest_trade",
            ]:
                val = latest.get(k)
                if val is None:
                    continue
                if k == "latest_trade" and isinstance(val, (int, float)):
                    text = f"{k}: {val}m"
                else:
                    text = f"{k}: {val}"
                if diff and diff.get(k) is not None:
                    dv = diff[k]
                    if dv > 0:
                        sign = "+"
                        arrow = "\u2b06"  # upward arrow
                    elif dv < 0:
                        sign = ""
                        arrow = "\u2b07"  # downward arrow
                    else:
                        sign = ""
                        arrow = ""
                    text += f" ({arrow}{sign}{dv})"
                lines.append(md(text))
            await q.edit_message_text(
                "\n".join(lines), parse_mode="MarkdownV2", reply_markup=sig_kb()
            )

    elif d == "manage_usr":
        await q.edit_message_text("User menu:", reply_markup=usr_kb())

    elif d == "usr_list":
        rows = await db.list_users()
        if rows:
            lines = [f"{'⭐' if r['admin'] else '▫'} {r['id']} {md(r['name'] or '')}" for r in rows]
            text = "📜 *Users*:\n" + "\n".join(lines)
        else:
            text = "ℹ None"
        await q.edit_message_text(text, parse_mode="MarkdownV2", reply_markup=usr_kb())

    elif d == "back":
        await q.edit_message_text("Menu:", reply_markup=main_kb())

    elif d in ("sig_add", "sig_del", "usr_add", "usr_del", "usr_toggle"):
        ctx.user_data["await"] = d
        prompt = {
            "sig_add": "Send full signal URL.",
            "sig_del": "Send signal ID to remove.",
            "usr_add": "Send: <telegram_id>|<name or note>",
            "usr_del": "Send user ID to remove.",
            "usr_toggle": "Send user ID to promote/demote.",
        }[d]
        await q.edit_message_text(
            prompt,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="back")]])
        )

async def text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    act = ctx.user_data.pop("await", None)
    txt = update.message.text.strip()
    me = update.effective_user

    if not await db.is_admin(me.id):
        await update.message.reply_text("⛔ Unauthorized")
        return
    if not act:
        await update.message.reply_text("Use the buttons.")
        return

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

# ---------- bootstrap ----------
if __name__ == "__main__":
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN missing")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(db.init())
    loop.run_until_complete(ensure_root())

    app = Application.builder().token(BOT_TOKEN).build()
    APP = app
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(menu_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text))

    loop.create_task(periodic_scrape())

    app.run_webhook(
        listen="0.0.0.0", port=PORT, url_path="telegram",
        webhook_url=f"{WEBHOOK_URL}/telegram",
        drop_pending_updates=True,
    )
