import logging

from datetime import datetime


logger = logging.getLogger(__name__)


def print_time_duration(tag: str, start: datetime, end: datetime):
    """
    Print out time duration between two dates.
    """

    time_diff = end - start

    days, seconds = time_diff.days, time_diff.seconds

    # The usage of `//` is require to ignore the remainding value
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    duration_msg = ''

    if hours > 0:
        duration_msg += f'{hours} hour(s), '

    if minutes > 0:
        duration_msg += f'{minutes} minute(s), '

    if seconds > 0:
        duration_msg += f'{seconds} second(s)'

    if not duration_msg:
        duration_msg += f'{time_diff.microseconds / 1000000} second(s)'

    logger.info(f" {tag} is completed in: {duration_msg}")
