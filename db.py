import aiosqlite
import pathlib

DB_PATH = pathlib.Path("data/database.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

_SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS signals (
    id   TEXT PRIMARY KEY,
    url  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS users (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    description TEXT,
    is_admin    INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS signal_history (
    sig_id         TEXT,
    ts             INTEGER,
    name           TEXT,
    drawdown       REAL,
    monthly_growth REAL,
    weeks          INTEGER,
    growth         REAL,
    trades         INTEGER,
    profit_trades  INTEGER,
    loss_trades    INTEGER,
    PRIMARY KEY(sig_id, ts)
);
"""

async def init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(_SCHEMA)
        await db.commit()

# -------- users --------
async def add_user(uid: int, name=None, desc=None, admin=False) -> bool:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO users VALUES (?, ?, ?, ?)",
                (uid, name, desc, int(admin)),
            )
            await db.commit()
        return True
    except aiosqlite.IntegrityError:
        return False

async def remove_user(uid: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("DELETE FROM users WHERE id = ?", (uid,))
        await db.commit()
        return cur.rowcount

async def is_admin(uid: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM users WHERE id = ? AND is_admin = 1", (uid,)
        )
        return await cur.fetchone() is not None

async def set_admin(uid: int, value: bool):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET is_admin = ? WHERE id = ?", (int(value), uid)
        )
        await db.commit()

async def list_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, name, description, is_admin FROM users ORDER BY id"
        )
        rows = await cur.fetchall()
        return [
            {"id": r[0], "name": r[1], "desc": r[2], "admin": bool(r[3])}
            for r in rows
        ]

# -------- signals --------
async def list_signals():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id, url FROM signals ORDER BY id")
        rows = await cur.fetchall()
        return [{"id": r[0], "url": r[1]} for r in rows]

async def signal_exists(sig_id: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM signals WHERE id = ?", (sig_id,))
        return await cur.fetchone() is not None

async def add_signal(sig_id: str, url: str) -> bool:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT INTO signals VALUES (?, ?)", (sig_id, url))
            await db.commit()
        return True
    except aiosqlite.IntegrityError:
        return False

async def remove_signal(sig_id: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("DELETE FROM signals WHERE id = ?", (sig_id,))
        await db.commit()
        return cur.rowcount

# -------- signal history --------
async def add_history(sig_id: str, **data):
    cols = ["sig_id", "ts", "name", "drawdown", "monthly_growth",
            "weeks", "growth", "trades", "profit_trades", "loss_trades"]
    values = [sig_id, data.get("ts"), data.get("name"), data.get("drawdown"),
              data.get("monthly_growth"), data.get("weeks"), data.get("growth"),
              data.get("trades"), data.get("profit_trades"), data.get("loss_trades")]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"INSERT OR IGNORE INTO signal_history ({', '.join(cols)})"
            f" VALUES ({', '.join(['?']*len(cols))})",
            values,
        )
        await db.commit()

async def latest_history(sig_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT ts,name,drawdown,monthly_growth,weeks,growth,trades,"
            "profit_trades,loss_trades FROM signal_history WHERE sig_id=?"
            " ORDER BY ts DESC LIMIT 1",
            (sig_id,)
        )
        row = await cur.fetchone()
        if row:
            keys = ["ts","name","drawdown","monthly_growth","weeks","growth",
                    "trades","profit_trades","loss_trades"]
            return dict(zip(keys, row))
        return None

async def previous_history(sig_id: str, before_ts: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT ts,name,drawdown,monthly_growth,weeks,growth,trades,"
            "profit_trades,loss_trades FROM signal_history WHERE sig_id=?"
            " AND ts<? ORDER BY ts DESC LIMIT 1",
            (sig_id, before_ts)
        )
        row = await cur.fetchone()
        if row:
            keys = ["ts","name","drawdown","monthly_growth","weeks","growth",
                    "trades","profit_trades","loss_trades"]
            return dict(zip(keys, row))
        return None

async def history_diff(sig_id: str):
    latest = await latest_history(sig_id)
    if not latest:
        return None
    prev = await previous_history(sig_id, latest["ts"])
    diff = None
    if prev:
        diff = {}
        for k, v in latest.items():
            if k in {"ts", "name"}:
                continue
            pv = prev.get(k)
            diff[k] = (v - pv) if (v is not None and pv is not None) else None
    return {"latest": latest, "previous": prev, "diff": diff}

