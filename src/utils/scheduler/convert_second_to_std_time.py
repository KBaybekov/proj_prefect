"""
Конвертация времени из абсолютного в стандартный
"""
import datetime


def convert_timestamp_to_std_time(time:int):
    return datetime.datetime.fromtimestamp(time)


print('eligible_time:', convert_timestamp_to_std_time(1762937587))
print('end_time:', convert_timestamp_to_std_time(1763196787))
