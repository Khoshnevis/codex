import asyncio
import logging
import os
import random
import re
from contextlib import suppress

import aiohttp
import telegram.error
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (Application, CallbackQueryHandler, CommandHandler,
                          ContextTypes, MessageHandler, filters)
from telegram.helpers import escape_markdown

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

def md(text: str) -> str:
    return escape_markdown(str(text), version=1)


def md2(text: str) -> str:
    """Escape text for MarkdownV2."""
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

async def stats_kb(rows):
    kb = []
    for r in rows:
        latest = await db.latest_history(r["id"])
        growth = latest.get("growth") if latest else None
        drawdown = latest.get("drawdown") if latest else None
        year = r.get("start_year") or (
            latest.get("start_year") if latest else None
        )
        prefix = 'A' if r.get('auto') else 'M'
        label = " - ".join(
            [
                f"{prefix} {r['id']}",
                r.get("name") or "?",
                str(year) if year is not None else "?",
                f"{growth}%" if growth is not None else "?",
                f"{drawdown}%" if drawdown is not None else "?",
            ]
        )
        kb.append([
            InlineKeyboardButton(label, callback_data=f"stat_{r['id']}")
        ])
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
    async with scraper.session() as sess:
        for idx, r in enumerate(rows):
            try:
                data = await scraper.scrape(r["url"], session=sess)
                await db.add_history(r["id"], **data)
                await db.update_signal_info(
                    r["id"],
                    name=data.get("name"),
                    weeks=data.get("weeks"),
                    latest_trade=data.get("latest_trade"),
                    start_year=data.get("start_year"),
                )
                info = await db.history_diff(r["id"])
                if info and info.get("diff"):
                    changes = []
                    for k, dv in info["diff"].items():
                        if dv:
                            changes.append(f"{k}: {dv:+}")
                    if changes:
                        text = (
                            f"\u2139 Updates for {info['latest']['name']} ({r['id']}):\n"
                            + "\n".join(changes)
                        )
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
    uid = update.effective_user.id
    if not (await db.is_admin(uid) or await db.user_exists(uid)):
        await update.message.reply_text("⛔ Unauthorized")
        return
    await update.message.reply_text("Welcome!", reply_markup=main_kb())

async def me_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    lines = [
        f"*id:* {u.id}",
        f"*first name:* {md(u.first_name)}" if u.first_name else None,
        f"*last name:* {md(u.last_name)}" if u.last_name else None,
        f"*username:* @{md(u.username)}" if u.username else None,
        f"*language:* {md(u.language_code)}" if u.language_code else None,
        f"*is_bot:* {u.is_bot}",
    ]
    text = "\n".join(filter(None, lines)) or "No data"
    await update.message.reply_text(text, parse_mode="Markdown")

async def menu_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    try:
        await q.answer()
    except telegram.error.BadRequest as e:
        if "query is too old" not in str(e).lower():
            raise
    d = q.data

    uid = q.from_user.id
    is_admin = await db.is_admin(uid)
    if not (is_admin or await db.user_exists(uid)):
        await q.edit_message_text("⛔ Unauthorized")
        return

    if d == "manage_sig":
        await q.edit_message_text("Signal menu:", reply_markup=sig_kb())

    elif d == "sig_list":
        rows = await db.list_signals()
        if rows:
            lines = [f"{'A' if r['auto'] else 'M'} {r['id']} → {md(r['url'])}" for r in rows]
            text = "📜 *Signals*:\n" + "\n".join(lines)
        else:
            text = "ℹ None"
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=sig_kb())

    elif d == "sig_stats":
        rows = await db.list_signals()
        if not rows:
            await q.edit_message_text("ℹ None", reply_markup=sig_kb())
        else:
            info_lines = []
            for r in rows:
                hist = await db.latest_history(r["id"])
                growth = hist.get("growth") if hist else None
                start = r.get("start_year") or (hist.get("start_year") if hist else None)
                gtxt = f"{growth}%" if growth is not None else "?"
                stxt = str(start) if start is not None else "?"
                prefix = 'A' if r.get('auto') else 'M'
                info_lines.append(f"{prefix} {r['id']} - {md(r.get('name') or '')} - {stxt} - {gtxt}")
            text = "📜 *Signals*:\n" + "\n".join(info_lines)
            await q.edit_message_text(
                "Select signal:",
                reply_markup=await stats_kb(rows),
            )

    elif d.startswith("stat_"):
        sid = d.split("_", 1)[1]
        info = await db.history_diff(sid)
        if not info:
            await q.edit_message_text("No history.", reply_markup=sig_kb())
        else:
            latest = info["latest"]
            diff = info["diff"]
            lines = [f"*{md2(latest['name'])}* \\({sid}\\)"]
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
                lines.append(md2(text))
            await q.edit_message_text(
                "\n".join(lines), parse_mode="MarkdownV2", reply_markup=sig_kb()
            )

    elif d == "manage_usr":
        await q.edit_message_text("User menu:", reply_markup=usr_kb())

    elif d == "usr_list":
        rows = await db.list_users()
        if rows:
            lines = [f"{'⭐' if r['admin'] else '▫'} {r['id']} {md2(r['name'] or '')}" for r in rows]
            text = "📜 *Users*:\n" + "\n".join(lines)
        else:
            text = "ℹ None"
        await q.edit_message_text(text, parse_mode="MarkdownV2", reply_markup=usr_kb())

    elif d == "back":
        await q.edit_message_text("Menu:", reply_markup=main_kb())

    elif d in ("sig_add", "sig_del", "usr_add", "usr_del", "usr_toggle"):
        if d in ("usr_add", "usr_del", "usr_toggle") and not is_admin:
            await q.edit_message_text("⛔ Unauthorized")
            return
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

    uid = me.id
    is_admin = await db.is_admin(uid)
    if not (is_admin or await db.user_exists(uid)):
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
        await db.add_signal(sid, url, auto=False)
        await update.message.reply_text("Added.", reply_markup=main_kb())

    elif act == "sig_del":
        sid = re.sub(r"\D", "", txt)
        await db.remove_signal(sid)
        await update.message.reply_text("Removed.", reply_markup=main_kb())

    elif act == "usr_add":
        if not is_admin:
            return await update.message.reply_text("⛔ Unauthorized")
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
        if not is_admin:
            return await update.message.reply_text("⛔ Unauthorized")
        uid_str = re.sub(r"\D", "", txt)
        if not uid_str:
            return await update.message.reply_text("Need ID.")
        if int(uid_str) == me.id:
            return await update.message.reply_text("Can't remove yourself.")
        await db.remove_user(int(uid_str))
        await update.message.reply_text("Removed.", reply_markup=main_kb())

    elif act == "usr_toggle":
        if not is_admin:
            return await update.message.reply_text("⛔ Unauthorized")
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


async def setcookie_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await db.is_admin(uid):
        await update.message.reply_text("⛔ Unauthorized")
        return
    text = update.message.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /setcookie <cookie>")
        return
    cookie = parts[1].strip()
    await db.set_auth_cookie(cookie)
    await update.message.reply_text("Cookie saved.")


async def testcookie_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await db.is_admin(uid):
        await update.message.reply_text("⛔ Unauthorized")
        return
    cookie = await db.get_auth_cookie()
    if not cookie:
        return await update.message.reply_text("No cookie set.")
    async with scraper.session() as sess:
        ok = await scraper.test_cookie(session=sess)
    await update.message.reply_text("✅ Cookie valid" if ok else "❌ Cookie invalid")


async def syncsubs_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await db.is_admin(uid):
        await update.message.reply_text("⛔ Unauthorized")
        return
    cookie = await db.get_auth_cookie()
    if not cookie:
        await update.message.reply_text("No cookie set.")
        return
    await update.message.reply_text("Fetching subscriptions…")
    async with scraper.session() as sess:
        if not await scraper.test_cookie(session=sess):
            await update.message.reply_text("❌ Cookie invalid")
            return
        subs = await scraper.list_subscriptions(session=sess)
    added = 0
    for s in subs:
        if not await db.signal_exists(s["id"]):
            await db.add_signal(s["id"], s["url"], name=s.get("name"), auto=True)
            added += 1
    await update.message.reply_text(
        f"Found {len(subs)} subscription(s). Added {added} new signal(s)."
    )

async def showcookie_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await db.is_admin(uid):
        await update.message.reply_text("⛔ Unauthorized")
        return
    cookie = await db.get_auth_cookie()
    text = cookie if cookie else "No cookie set."
    await update.message.reply_text(text)

async def balance_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await db.is_admin(uid):
        await update.message.reply_text("⛔ Unauthorized")
        return
    cookie = await db.get_auth_cookie()
    if not cookie:
        await update.message.reply_text("No cookie set.")
        return
    async with scraper.session() as sess:
        bal, locked = await scraper.fetch_balance(session=sess)
    if bal is None:
        await update.message.reply_text("Failed to fetch balance.")
    else:
        locked_txt = f" (locked: {locked})" if locked is not None else ""
        await update.message.reply_text(f"Balance: {bal}{locked_txt}")

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
    app.add_handler(CommandHandler("me", me_cmd))
    app.add_handler(CommandHandler("setcookie", setcookie_cmd))
    app.add_handler(CommandHandler("testcookie", testcookie_cmd))
    app.add_handler(CommandHandler("syncsubs", syncsubs_cmd))
    app.add_handler(CommandHandler("showcookie", showcookie_cmd))
    app.add_handler(CommandHandler("balance", balance_cmd))
    app.add_handler(CallbackQueryHandler(menu_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text))

    loop.create_task(periodic_scrape())

    app.run_webhook(
        listen="0.0.0.0", port=PORT, url_path="telegram",
        webhook_url=f"{WEBHOOK_URL}/telegram",
        drop_pending_updates=True,
    )
