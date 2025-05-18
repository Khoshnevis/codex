import re, time, aiohttp
from bs4 import BeautifulSoup

async def fetch_html(url: str) -> str:
    async with aiohttp.ClientSession() as s:
        async with s.get(url) as r:
            r.raise_for_status()
            return await r.text()

def _num(text):
    if text is None:
        return None
    t = re.sub(r"[^0-9.+-]", "", text)
    try:
        return float(t)
    except ValueError:
        return None

async def scrape(url: str) -> dict:
    html = await fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    def by_label(label_text):
        label = soup.find("div", class_="s-list-info__label", string=label_text)
        if label and label.parent:
            return label.parent.find("div", class_="s-list-info__value")
        return None

    def stats_label(text):
        lab = soup.find("div", class_="s-data-columns__label", string=text)
        if lab and lab.parent:
            return lab.parent.find("div", class_="s-data-columns__value")
        return None

    name_el = soup.find("h1", class_="title-min")
    name = name_el.get_text(strip=True) if name_el else None

    growth = _num((by_label("Growth:") or {}).get_text() if by_label("Growth:") else None)
    weeks = _num((by_label("Weeks:") or {}).get_text())
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
        "trades": int(trades) if trades is not None else None,
        "profit_trades": int(profit_trades) if profit_trades is not None else None,
        "loss_trades": int(loss_trades) if loss_trades is not None else None,
    }
