import re
import time
import aiohttp
from bs4 import BeautifulSoup

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

async def fetch_html(url: str) -> str:
    async with aiohttp.ClientSession(headers={"User-Agent": UA}) as s:
        async with s.get(url) as r:
            r.raise_for_status()
            return await r.text()

def _num(text):
    if text is None:
        return None
    s = text.strip()
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    # remove common separators/whitespace
    s = s.replace(",", "").replace(" ", "")
    m = re.search(r"[-+]?\d*\.?\d+", s)
    if not m:
        return None
    try:
        num = float(m.group(0))
    except ValueError:
        return None
    return -num if neg else num

async def scrape(url: str) -> dict:
    html = await fetch_html(url)
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
    drawdown = _num((stats_label("By Equity:") or {}).get_text())
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
