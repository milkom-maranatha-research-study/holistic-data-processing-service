
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

from pandas import DataFrame, Series
from typing import Dict, List


class NumOfTherapistMapper:

    def to_weekly_therapists_map(self, dataframe: DataFrame) -> Dict:
        """
        Converts that given `dataframe` into a weekly number of therapist
        dictionary per organization.
        """
        map = {}

        for _, row in dataframe.iterrows():

            # Row[0] is defined by `{period},{org_id},{total_ther_in_org}`
            _, org_id, _ = row[0].split(',')

            if map.get(org_id):
                existing = map[org_id]

                map[org_id] = existing + self._get_weekly_num_of_ther(row)

                continue
            
            map[org_id] = self._get_weekly_num_of_ther(row)

        return map

    def to_monthly_therapists_map(self, dataframe: DataFrame) -> Dict:
        """
        Converts that given `dataframe` into a monthly number of therapist
        dictionary per organization.
        """
        map = {}

        for _, row in dataframe.iterrows():

            # Row[0] is defined by `{period},{org_id},{total_ther_in_org}`
            _, org_id, _ = row[0].split(',')

            if map.get(org_id):
                existing = map[org_id]

                map[org_id] = existing + self._get_monthly_num_of_ther(row)

                continue
            
            map[org_id] = self._get_monthly_num_of_ther(row)

        return map

    def to_yearly_therapists_map(self, dataframe: DataFrame) -> Dict:
        """
        Converts that given `dataframe` into a yearly number of therapist
        dictionary per organization.
        """
        map = {}

        for _, row in dataframe.iterrows():

            # Row[0] is defined by `{period},{org_id},{total_ther_in_org}`
            _, org_id, _ = row[0].split(',')

            if map.get(org_id):
                existing = map[org_id]

                map[org_id] = existing + self._get_yearly_num_of_ther(row)

                continue
            
            map[org_id] = self._get_yearly_num_of_ther(row)

        return map

    def _get_weekly_num_of_ther(self, row: Series) -> List[Dict]:
        """
        Converts that given `row` into a list of number of therapist dictionary.
        """

        # Row[0] is defined by `{period},{org_id},{total_ther_in_org}`
        period, _, _ = row[0].split(',')
        period_start, period_end = period.split('/')

        # Row[1] is defined by `{total_active_ther},{total_inactive_ther}
        total_active_ther, total_inactive_ther = row[1].split(',')

        return [
            {
                'period_type': 'weekly',
                'start_date': period_start,
                'end_date': period_end,
                'is_active': True,
                'value': total_active_ther
            },
            {
                'period_type': 'weekly',
                'start_date': period_start,
                'end_date': period_end,
                'is_active': False,
                'value': total_inactive_ther
            }
        ]

    def _get_monthly_num_of_ther(self, row: Series) -> List[Dict]:
        """
        Converts that given `row` into a list of number of therapist dictionary.
        """

        # Row[0] is defined by `{period},{org_id},{total_ther_in_org}`
        period, _, _ = row[0].split(',')
        period_start = parse(f'{period}-01', yearfirst=True)
        period_end = period_start + relativedelta(day=31)

        str_period_start = period_start.strftime('%Y-%m-%d')
        str_period_end = period_end.strftime('%Y-%m-%d')

        # Row[1] is defined by `{total_active_ther},{total_inactive_ther}
        total_active_ther, total_inactive_ther = row[1].split(',')

        return [
            {
                'period_type': 'monthly',
                'start_date': str_period_start,
                'end_date': str_period_end,
                'is_active': True,
                'value': total_active_ther
            },
            {
                'period_type': 'monthly',
                'start_date': str_period_start,
                'end_date': str_period_end,
                'is_active': False,
                'value': total_inactive_ther
            }
        ]

    def _get_yearly_num_of_ther(self, row: Series) -> List[Dict]:
        """
        Converts that given `row` into a list of number of therapist dictionary.
        """

        # Row[0] is defined by `{period},{org_id},{total_ther_in_org}`
        period, _, _ = row[0].split(',')
        period_start = parse(f'{period}-01-01', yearfirst=True)
        period_end = parse(f'{period}-12-01', yearfirst=True) + relativedelta(day=31)

        str_period_start = period_start.strftime('%Y-%m-%d')
        str_period_end = period_end.strftime('%Y-%m-%d')

        # Row[1] is defined by `{total_active_ther},{total_inactive_ther}
        total_active_ther, total_inactive_ther = row[1].split(',')

        return [
            {
                'period_type': 'yearly',
                'start_date': str_period_start,
                'end_date': str_period_end,
                'is_active': True,
                'value': total_active_ther
            },
            {
                'period_type': 'yearly',
                'start_date': str_period_start,
                'end_date': str_period_end,
                'is_active': False,
                'value': total_inactive_ther
            }
        ]
