import aiohttp, calendar, re
from datetime import datetime
from bs4 import BeautifulSoup

MONTH_ABBR = {calendar.month_abbr[i].lower(): i for i in range(1, 13)}

async def fetch_signal_data(url: str) -> dict[str, str]:
    """Grab name, maximum DD, weeks online and current-month growth from an MQL5
    signal page.  All keys are always returned (dash if not found)."""
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=15)
    ) as sess, sess.get(url, allow_redirects=True) as resp:

        html = await resp.text()
        soup = BeautifulSoup(html, "lxml")

        # -------- name --------
        name = soup.find("h1")
        name_text = name.get_text(strip=True) if name else soup.title.get_text(strip=True)

        # -------- maximum draw-down --------
        m_dd = re.search(r"Max(?:imum)?\s+drawdown[^0-9]*([\d.]+%)", html, re.I)
        dd_text = m_dd.group(1) if m_dd else "-"

        # -------- number of weeks online --------
        m_weeks = re.search(r"(\d+)\s+weeks?", html, re.I)
        weeks = m_weeks.group(1) if m_weeks else "-"

        # -------- current-month growth --------
        now = datetime.utcnow()
        month_label = f"{calendar.month_abbr[now.month]} {now.year}".lower()

        growth = "-"
        for tr in soup.select("table tr"):
            tds = tr.find_all("td")
            if len(tds) >= 2 and month_label in tds[0].get_text(strip=True).lower():
                growth = tds[1].get_text(strip=True)
                break

        return {
            "name": name_text,
            "drawdown": dd_text,
            "weeks": weeks,
            "growth": growth,
        }
