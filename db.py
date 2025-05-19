import aiosqlite
import pathlib

DB_PATH = pathlib.Path("data/database.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

_SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS signals (
    id   TEXT PRIMARY KEY,
    url  TEXT NOT NULL,
    name TEXT,
    weeks INTEGER,
    latest_trade INTEGER,
    start_year INTEGER
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
    start_year     INTEGER,
    latest_trade   INTEGER,
    PRIMARY KEY(sig_id, ts)
);
"""

async def init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(_SCHEMA)
        # migrations for additional columns
        for stmt in [
            "ALTER TABLE signals ADD COLUMN name TEXT",
            "ALTER TABLE signals ADD COLUMN weeks INTEGER",
            "ALTER TABLE signals ADD COLUMN latest_trade INTEGER",
            "ALTER TABLE signals ADD COLUMN start_year INTEGER",
            "ALTER TABLE signal_history ADD COLUMN start_year INTEGER",
            "ALTER TABLE signal_history ADD COLUMN latest_trade INTEGER",
        ]:
            try:
                await db.execute(stmt)
            except aiosqlite.Error:
                pass
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

async def list_user_ids():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id FROM users")
        rows = await cur.fetchall()
        return [r[0] for r in rows]

# -------- signals --------
async def list_signals():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id, url, name, weeks, latest_trade, start_year FROM signals ORDER BY id")
        rows = await cur.fetchall()
        return [
            {"id": r[0], "url": r[1], "name": r[2], "weeks": r[3], "latest_trade": r[4], "start_year": r[5]}
            for r in rows
        ]

async def signal_exists(sig_id: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM signals WHERE id = ?", (sig_id,))
        return await cur.fetchone() is not None

async def add_signal(sig_id: str, url: str, name=None, weeks=None, latest_trade=None, start_year=None) -> bool:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO signals (id,url,name,weeks,latest_trade,start_year) VALUES (?,?,?,?,?,?)",
                (sig_id, url, name, weeks, latest_trade, start_year),
            )
            await db.commit()
        return True
    except aiosqlite.IntegrityError:
        return False

async def remove_signal(sig_id: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("DELETE FROM signals WHERE id = ?", (sig_id,))
        await db.commit()
        return cur.rowcount

async def update_signal_info(sig_id: str, **kwargs):
    cols = []
    values = []
    for k in ["name", "weeks", "latest_trade", "start_year"]:
        if k in kwargs and kwargs[k] is not None:
            cols.append(f"{k} = ?")
            values.append(kwargs[k])
    if not cols:
        return
    values.append(sig_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"UPDATE signals SET {', '.join(cols)} WHERE id = ?",
            values,
        )
        await db.commit()

# -------- signal history --------
async def add_history(sig_id: str, **data):
    cols = ["sig_id", "ts", "name", "drawdown", "monthly_growth",
            "weeks", "growth", "trades", "profit_trades", "loss_trades",
            "start_year", "latest_trade"]
    values = [sig_id, data.get("ts"), data.get("name"), data.get("drawdown"),
              data.get("monthly_growth"), data.get("weeks"), data.get("growth"),
              data.get("trades"), data.get("profit_trades"), data.get("loss_trades"),
              data.get("start_year"), data.get("latest_trade")]
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
            "profit_trades,loss_trades,start_year,latest_trade FROM signal_history WHERE sig_id=?"
            " ORDER BY ts DESC LIMIT 1",
            (sig_id,)
        )
        row = await cur.fetchone()
        if row:
            keys = ["ts","name","drawdown","monthly_growth","weeks","growth",
                    "trades","profit_trades","loss_trades","start_year","latest_trade"]
            return dict(zip(keys, row))
        return None

async def previous_history(sig_id: str, before_ts: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT ts,name,drawdown,monthly_growth,weeks,growth,trades,"
            "profit_trades,loss_trades,start_year,latest_trade FROM signal_history WHERE sig_id=?"
            " AND ts<? ORDER BY ts DESC LIMIT 1",
            (sig_id, before_ts)
        )
        row = await cur.fetchone()
        if row:
            keys = ["ts","name","drawdown","monthly_growth","weeks","growth",
                    "trades","profit_trades","loss_trades","start_year","latest_trade"]
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


