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

