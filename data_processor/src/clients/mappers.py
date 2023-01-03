from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from itertools import chain

from pandas import DataFrame, Series
from typing import Dict, List


class TotalAllTherapistMapper:

    def to_total_thers_map(self, dataframe: DataFrame) -> Dict:
        """
        Converts that given `dataframe` into the all-time total therapists.

        Dataframe's row is defined by:
        ```
        [{period},{total_active_thers},{total_inactive_thers},{total_thers}]
        """
        num_of_ther_lists = [
            self._get_total_thers(row)
            for _, row in dataframe.iterrows()
        ]

        return list(chain.from_iterable(num_of_ther_lists))

    def _get_total_thers(self, row: Series) -> List[Dict]:
        """
        Converts that given `row` into a list of the total therapists dictionary.

        Dataframe's row is defined by:
        ```
        [{period},{total_active_thers},{total_inactive_thers},{total_thers}]
        """
        period_start, period_end = row[0].split('/')

        return [
            {
                'start_date': period_start,
                'end_date': period_end,
                'is_active': True,
                'value': int(row[1])
            },
            {
                'start_date': period_start,
                'end_date': period_end,
                'is_active': False,
                'value': int(row[2])
            }
        ]


class TotalTherapistMapper:

    def to_nd_total_thers(self, dataframe: DataFrame, period_type: str) -> List[Dict]:
        """
        Converts that given `dataframe` into a list of the total therapists
        in NiceDay on that specific `period_type`.

        Dataframe's row is defined by:
        ```
        [{period},{active_ther},{inactive_ther},{total_ther}]
        """
        total_ther_lists = [
            self._get_nd_total_thers(row, period_type)
            for _, row in dataframe.iterrows()
        ]

        return list(chain.from_iterable(total_ther_lists))

    def to_org_total_thers_map(self, dataframe: DataFrame, period_type: str) -> Dict:
        """
        Converts that given `dataframe` into the total therapists
        per organization dictionary for that specific `period_type`
        grouped by the organization id.

        Dataframe's row is defined by:
        ```
        [{period},{org_id},{active_ther},{inactive_ther},{total_ther}]
        """
        map = {}

        for _, row in dataframe.iterrows():

            org_id = int(row[1])
            org_num_of_thers = self._get_org_total_thers(row, period_type)

            if map.get(org_id):
                existing = map[org_id]

                map[org_id] = existing + org_num_of_thers

                continue

            map[org_id] = org_num_of_thers

        return map

    def _get_org_total_thers(self, row: Series, period_type: str) -> List[Dict]:
        """
        Converts that given `row` into a list of the total therapists dictionary.
        """
        if period_type == 'weekly':
            return self._get_org_weekly_total_thers(row)

        elif period_type == 'monthly':
            return self._get_org_monthly_total_thers(row)

        elif period_type == 'yearly':
            return self._get_org_yearly_total_thers(row)

        return []

    def _get_org_weekly_total_thers(self, row: Series) -> List[Dict]:
        """
        Converts that given `row` into a list of the total therapists dictionary.

        Row is defined by:
        ```
        [{period},{org_id},{active_ther},{inactive_ther},{total_ther}]
        ```
        """

        period = row[0]
        period_start, period_end = period.split('/')

        return [
            {
                'period_type': 'weekly',
                'start_date': period_start,
                'end_date': period_end,
                'is_active': True,
                'value': int(row[2])
            },
            {
                'period_type': 'weekly',
                'start_date': period_start,
                'end_date': period_end,
                'is_active': False,
                'value': int(row[3])
            }
        ]

    def _get_org_monthly_total_thers(self, row: Series) -> List[Dict]:
        """
        Converts that given `row` into a list of the total therapists dictionary.

        Row is defined by:
        ```
        [{period},{org_id},{active_ther},{inactive_ther},{total_ther}]
        ```
        """
        period = row[0]
        period_start = parse(f'{period}-01', yearfirst=True)
        period_end = period_start + relativedelta(day=31)

        str_period_start = period_start.strftime('%Y-%m-%d')
        str_period_end = period_end.strftime('%Y-%m-%d')

        return [
            {
                'period_type': 'monthly',
                'start_date': str_period_start,
                'end_date': str_period_end,
                'is_active': True,
                'value': int(row[2])
            },
            {
                'period_type': 'monthly',
                'start_date': str_period_start,
                'end_date': str_period_end,
                'is_active': False,
                'value': int(row[3])
            }
        ]

    def _get_org_yearly_total_thers(self, row: Series) -> List[Dict]:
        """
        Converts that given `row` into a list of the total therapists dictionary.

        Row is defined by:
        ```
        [{period},{org_id},{active_ther},{inactive_ther},{total_ther}]
        ```
        """

        period = row[0]
        period_start = parse(f'{period}-01-01', yearfirst=True)
        period_end = parse(f'{period}-12-01', yearfirst=True) + relativedelta(day=31)

        str_period_start = period_start.strftime('%Y-%m-%d')
        str_period_end = period_end.strftime('%Y-%m-%d')

        return [
            {
                'period_type': 'yearly',
                'start_date': str_period_start,
                'end_date': str_period_end,
                'is_active': True,
                'value': int(row[2])
            },
            {
                'period_type': 'yearly',
                'start_date': str_period_start,
                'end_date': str_period_end,
                'is_active': False,
                'value': int(row[3])
            }
        ]

    def _get_nd_total_thers(self, row: Series, period_type: str) -> List[Dict]:
        """
        Converts that given `row` into a list of the total therapists dictionary.
        """
        if period_type == 'weekly':
            return self._get_nd_weekly_total_thers(row)

        elif period_type == 'monthly':
            return self._get_nd_monthly_total_thers(row)

        elif period_type == 'yearly':
            return self._get_nd_yearly_total_thers(row)

        return []

    def _get_nd_weekly_total_thers(self, row: Series) -> List[Dict]:
        """
        Converts that given `row` into a list of the total therapists dictionary.

        Row is defined by:
        ```
        [{period},{active_ther},{inactive_ther},{total_ther}]
        ```
        """

        period = row[0]
        period_start, period_end = period.split('/')

        return [
            {
                'period_type': 'weekly',
                'start_date': period_start,
                'end_date': period_end,
                'is_active': True,
                'value': int(row[1])
            },
            {
                'period_type': 'weekly',
                'start_date': period_start,
                'end_date': period_end,
                'is_active': False,
                'value': int(row[2])
            }
        ]

    def _get_nd_monthly_total_thers(self, row: Series) -> List[Dict]:
        """
        Converts that given `row` into a list of the total therapists dictionary.

        Row is defined by:
        ```
        [{period},{active_ther},{inactive_ther},{total_ther}]
        ```
        """
        period = row[0]
        period_start = parse(f'{period}-01', yearfirst=True)
        period_end = period_start + relativedelta(day=31)

        str_period_start = period_start.strftime('%Y-%m-%d')
        str_period_end = period_end.strftime('%Y-%m-%d')

        return [
            {
                'period_type': 'monthly',
                'start_date': str_period_start,
                'end_date': str_period_end,
                'is_active': True,
                'value': int(row[1])
            },
            {
                'period_type': 'monthly',
                'start_date': str_period_start,
                'end_date': str_period_end,
                'is_active': False,
                'value': int(row[2])
            }
        ]

    def _get_nd_yearly_total_thers(self, row: Series) -> List[Dict]:
        """
        Converts that given `row` into a list of the total therapists dictionary.

        Row is defined by:
        ```
        [{period},{active_ther},{inactive_ther},{total_ther}]
        ```
        """

        period = row[0]
        period_start = parse(f'{period}-01-01', yearfirst=True)
        period_end = parse(f'{period}-12-01', yearfirst=True) + relativedelta(day=31)

        str_period_start = period_start.strftime('%Y-%m-%d')
        str_period_end = period_end.strftime('%Y-%m-%d')

        return [
            {
                'period_type': 'yearly',
                'start_date': str_period_start,
                'end_date': str_period_end,
                'is_active': True,
                'value': int(row[1])
            },
            {
                'period_type': 'yearly',
                'start_date': str_period_start,
                'end_date': str_period_end,
                'is_active': False,
                'value': int(row[2])
            }
        ]


class TherapistRateMapper:

    def to_nd_rates(self, dataframe: DataFrame, period_type: str) -> List[Dict]:
        """
        Converts that given `dataframe` into a list of the therapists' rates in NiceDay
        on that specific `period_type`.

        Dataframe's row is defined by:
        ```
        [{period_start},{period_end},{churn_rate},{retention_rate}]
        ```
        """
        rates = []

        for _, row in dataframe.iterrows():

            rates += [
                {
                    'period_type': period_type,
                    'start_date': f'{row[0]}',
                    'end_date': f'{row[1]}',
                    'type': 'churn_rate',
                    'rate_value': float(row[2])
                },
                {
                    'period_type': period_type,
                    'start_date': f'{row[0]}',
                    'end_date': f'{row[1]}',
                    'type': 'retention_rate',
                    'rate_value': float(row[3])
                }
            ]

        return rates

    def to_org_rates_map(self, dataframe: DataFrame, period_type: str) -> Dict:
        """
        Converts that given `dataframe` into a map of the therapists' rates
        based on that given `period_type` grouped by the organization id.

        Dataframe's row is defined by:
        ```
        [{period_start},{period_end},{org_id},{churn_rate},{retention_rate}]
        ```
        """
        map = {}

        for _, row in dataframe.iterrows():

            org_id = int(row[2])
            org_rates = self._get_rate(row, period_type)

            if map.get(org_id):
                existing = map[org_id]

                map[org_id] = existing + org_rates

                continue

            map[org_id] = org_rates

        return map

    def _get_rate(self, row: Series, period_type: str) -> List[Dict]:
        """
        Converts that given `row` into a list of therapists' rates dictionary.

        Row is defined by:
        ```
        [{period_start},{period_end},{org_id},{churn_rate},{retention_rate}]
        ```
        """

        return [
            {
                'period_type': period_type,
                'start_date': f'{row[0]}',
                'end_date': f'{row[1]}',
                'type': 'churn_rate',
                'rate_value': float(row[3])
            },
            {
                'period_type': period_type,
                'start_date': f'{row[0]}',
                'end_date': f'{row[1]}',
                'type': 'retention_rate',
                'rate_value': float(row[4])
            }
        ]
