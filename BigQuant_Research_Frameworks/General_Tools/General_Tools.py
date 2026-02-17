import dai
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta

def get_date_str(date_str: str) -> str:

    if not isinstance(date_str, str):
        raise ValueError("请检查输入日期字符串的格式")

    formats = [
        '%Y-%m-%d',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f'
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            break
        except ValueError:
            dt = None

    if dt is None:
        raise ValueError("请检查输入日期字符串的格式")

    if len(date_str) == 10:
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)

    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

