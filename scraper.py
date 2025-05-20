import re
import time

import aiohttp
from contextlib import asynccontextmanager
from bs4 import BeautifulSoup

import db

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)


async def create_session() -> aiohttp.ClientSession:
    """Create a preconfigured aiohttp session."""
    headers = {"User-Agent": UA}
    cookie = await db.get_auth_cookie()
    if cookie:
        headers["Cookie"] = cookie
    return aiohttp.ClientSession(headers=headers)


@asynccontextmanager
async def session() -> aiohttp.ClientSession:
    """Async context manager yielding a reusable session."""
    s = await create_session()
    try:
        yield s
    finally:
        await s.close()

async def fetch_html(url: str, session: aiohttp.ClientSession | None = None) -> str:
    """Fetch HTML using provided session or a temporary one."""
    own = session is None
    if own:
        session = await create_session()
    assert session is not None
    async with session.get(url) as r:
        r.raise_for_status()
        html = await r.text()
    if own:
        await session.close()
    return html


async def test_cookie(session: aiohttp.ClientSession | None = None) -> bool:
    own = session is None
    if own:
        session = await create_session()
    assert session is not None
    try:
        async with session.get(
            "https://www.mql5.com/en/signals/subscriptions",
            allow_redirects=True,
        ) as r:
            final = str(r.url)
            return r.status == 200 and "/en/signals/subscriptions" in final
    except Exception:
        return False
    finally:
        if own:
            await session.close()

async def list_subscriptions(session: aiohttp.ClientSession | None = None) -> list[dict]:
    html = await fetch_html("https://www.mql5.com/en/signals/subscriptions", session=session)
    soup = BeautifulSoup(html, "lxml")
    results = []
    table = soup.find("div", class_="signals-table")
    if not table:
        return results
    rows = table.find_all("div", class_="row")
    seen = set()
    for row in rows:
        link = None
        for a in row.find_all("a", href=True):
            href = a.get("href", "")
            m = re.match(r"^/(?:[a-z]{2}/)?signals/(?!subscription/)(\d+)", href)
            if m:
                link = a
                break
        if not link:
            continue
        sid = m.group(1)
        if sid in seen:
            continue
        seen.add(sid)
        url = "https://www.mql5.com" + link["href"]
        name = link.get_text(strip=True)
        results.append({"id": sid, "url": url, "name": name})
    return results

def _num(text):
    if text is None:
        return None
    s = text.strip()
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    s = s.replace(",", "").replace(" ", "")
    m = re.search(r"[-+]?\d*\.?\d+", s)
    if not m:
        return None
    try:
        num = float(m.group(0))
    except ValueError:
        return None
    return -num if neg else num

async def scrape(url: str, session: aiohttp.ClientSession | None = None) -> dict:
    html = await fetch_html(url, session=session)
    soup = BeautifulSoup(html, "lxml")

    def by_label(label_text):
        pat = re.compile(re.escape(label_text.strip()), re.I)
        label = soup.find("div", class_="s-list-info__label", string=pat)
        if label and label.parent:
            return label.parent.find("div", class_="s-list-info__value")
        return None

    def stats_label(text):
        pat = re.compile(re.escape(text.strip()), re.I)
        lab = soup.find("div", class_="s-data-columns__label", string=pat)
        if lab and lab.parent:
            return lab.parent.find("div", class_="s-data-columns__value")
        return None

    name_el = soup.find("h1", class_="title-min")
    name = name_el.get_text(strip=True) if name_el else None

    growth = _num((by_label("Growth:") or {}).get_text() if by_label("Growth:") else None)
    weeks = _num((by_label("Weeks:") or {}).get_text())
    start_year = None
    val = (by_label("Started:") or {}).get_text() if by_label("Started:") else None
    if val:
        m = re.search(r"(\d{4})", val)
        if m:
            start_year = int(m.group(1))
    latest_trade = None
    lt_val = (by_label("Latest trade:") or {}).get_text() if by_label("Latest trade:") else None
    if lt_val:
        m = re.search(r"(\d+)\s+(second|minute|hour|day|week|month)", lt_val)
        if m:
            num = int(m.group(1))
            unit = m.group(2).lower()
            mult = {
                "second": 1/60,
                "minute": 1,
                "hour": 60,
                "day": 60*24,
                "week": 60*24*7,
                "month": 60*24*30,
            }[unit]
            latest_trade = int(num * mult)
    drawdown = _num((stats_label("By Balance:") or {}).get_text())
    monthly = _num((stats_label("Monthly growth:") or {}).get_text())
    trades = _num((stats_label("Trades:") or {}).get_text())
    profit_trades = _num((stats_label("Profit Trades:") or {}).get_text())
    loss_trades = _num((stats_label("Loss Trades:") or {}).get_text())

    return {
        "ts": int(time.time()),
        "name": name,
        "growth": growth,
        "weeks": int(weeks) if weeks is not None else None,
        "drawdown": drawdown,
        "monthly_growth": monthly,
        "start_year": start_year,
        "latest_trade": latest_trade,
        "trades": int(trades) if trades is not None else None,
        "profit_trades": int(profit_trades) if profit_trades is not None else None,
        "loss_trades": int(loss_trades) if loss_trades is not None else None,
    }
