from datetime import date, datetime, timedelta

import pytz

FORMAT_YYYYMMDD = "%Y%m%d"
FORMAT_YYYYMMDDHHMMSS = "%Y%m%d%H%M%S"
FORMAT_YYYY_MM_DD = "%Y-%m-%d"
FORMAT_YYYY_MM_DD_HH_MM_SS = "%Y-%m-%d %H:%M:%S"

tz = pytz.timezone("Asia/Tokyo")


def get_today() -> date:
    return datetime.now(tz).date()


def get_yesterday() -> date:
    return datetime.now(tz).date() - timedelta(days=1)


def get_now() -> datetime:
    return datetime.now(tz)


def get_time_str(time: datetime = datetime.now(tz), format: str = FORMAT_YYYY_MM_DD_HH_MM_SS) -> str:
    return time.strftime(format)


def get_date_str(date_obj: date = date.today(), format: str = FORMAT_YYYY_MM_DD) -> str:
    return date_obj.strftime(format)


def str_to_date(date_str: str, format: str = FORMAT_YYYY_MM_DD) -> date:
    return datetime.strptime(date_str, format).astimezone(tz).date()


def str_to_datetime(date_str: str, format: str = FORMAT_YYYY_MM_DD_HH_MM_SS) -> datetime:
    return datetime.strptime(date_str, format).astimezone(tz)
