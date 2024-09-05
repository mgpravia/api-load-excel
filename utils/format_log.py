import pytz
import config.configuration as config
from datetime import datetime


def message_format(message):
    current_date = datetime.now(pytz.timezone(config.TIMEZONE)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{current_date} {message}")