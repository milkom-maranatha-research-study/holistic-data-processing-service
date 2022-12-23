from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Tuple


class DateUtil:

    def get_periods_from(self, start: datetime, end: datetime, period_type: str) -> List[Tuple]:
        """
        Returns list of date periods from the given `start` and `end` dates.

        The time range is specified by the `period_type`.
        """

        if start > end:
            raise ValueError(
                {start: "Start date must be less then or equals with end date."}
            )

        if period_type not in ['weekly', 'monthly']:
            raise ValueError(
                {period_type: "Unsupported period type. It must be 'weekly' or 'monthly'."}
            )

        if period_type == 'weekly':
            return self.get_weekly_periods(start, end)

        if period_type == 'monthly':
            return self.get_monthly_periods(start, end)

        return []

    def get_weekly_periods(self, start: datetime, end: datetime) -> List[Tuple]:
        """
        Returns a list of weekly periods from the given `start` and `end` dates.
        """

        if start > end:
            raise ValueError(
                {start: "Start date must be less then or equals with end date."}
            )

        periods = []

        while start <= end:
            start_of_week, end_of_week = self.get_week_period_of(start)

            periods.append((start_of_week, end_of_week))

            start = end_of_week + timedelta(days=1)

        return periods

    def get_monthly_periods(self, start: datetime, end: datetime) -> List[Tuple]:
        """
        Returns a list of monthly periods from the given `start` and `end` dates.
        """

        if start > end:
            raise ValueError(
                {start: "Start date must be less then or equals with end date."}
            )

        periods = []

        # Round `end` date
        if start.month != end.month:
            # We use `relativedelta` to calculate its end of month
            # So it won't blindly add 31 days to the `end` date.
            end = end.replace(day=1) + relativedelta(day=31)

        while start <= end:
            start_of_month, end_of_month = self.get_month_period_of(start)

            periods.append((start_of_month, end_of_month))

            start = end_of_month + timedelta(days=1)

        return periods
    
    def get_yearly_periods(self, start: datetime, end: datetime) -> List[Tuple]:
        """
        Returns a list of yearly periods from the given `start` and `end` dates.
        """

        if start > end:
            raise ValueError(
                {start: "Start date must be less then or equals with end date."}
            )

        periods = []

        # Round `end` date
        if start.year != end.year:
            # We use `relativedelta` to calculate its end of month
            # So it won't blindly add 31 days to the `end` date.
            end = end.replace(day=1) + relativedelta(months=11, day=31)

        while start <= end:
            start_of_year, end_of_year = self.get_year_period_of(start)

            periods.append((start_of_year, end_of_year))

            start = end_of_year + timedelta(days=1)

        return periods

    def get_week_period_of(self, date: datetime) -> Tuple:
        """
        Returns a week period of the given `date`.
        """

        start = date - timedelta(days=date.weekday())
        end = start + timedelta(days=6)

        return (start, end)

    def get_month_period_of(self, date: datetime) -> Tuple:
        """
        Returns a month period of the given `date`.
        """

        start = date.replace(day=1)

        # We use `relativedelta` to calculate the end of month
        # So it won't blindly add 31 days to the given `date`.
        end = start + relativedelta(day=31)

        return (start, end)

    def get_year_period_of(self, date: datetime) -> Tuple:
        """
        Returns a year period of the given `date`.
        """

        start = date.replace(day=1, month=1)

        # We use `relativedelta` to calculate the end date of the year
        # So it won't exceed the maximum date of December in that year.
        end = start + relativedelta(months=11, day=31)

        return (start, end)
