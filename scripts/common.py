from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd


def load_dotenv(env_path: str = ".env") -> None:
    path = Path(env_path)
    if not path.exists():
        return

    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def previous_business_day_str(timezone_name: str = "Asia/Tokyo") -> str:
    now = datetime.now(ZoneInfo(timezone_name))
    previous_day = pd.Timestamp(now.date()) - pd.tseries.offsets.BDay(1)
    return previous_day.strftime("%Y-%m-%d")
