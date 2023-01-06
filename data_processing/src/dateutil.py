from dateutil.relativedelta import relativedelta


def is_one_week_diff(previous, current):
    """
    Returns `True` if the days differences between `current`
    and `previous` date is 1 week.
    """
    if current < previous:
        return False

    return bool(
        relativedelta(current, previous).days == 7
    )


def is_one_month_diff(previous, current):
    """
    Returns `True` if the months differences between `current`
    and `previous` date is 1 month.
    """
    if current < previous:
        return False

    return bool(
        relativedelta(current, previous).months == 1
    )


def is_one_year_diff(previous, current):
    """
    Returns `True` if the years differences between `current`
    and `previous` date is 1 year.
    """
    if current < previous:
        return False

    return bool(
        relativedelta(current, previous).years == 1
    )
