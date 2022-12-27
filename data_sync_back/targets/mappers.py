
from pandas import DataFrame, Series
from typing import Dict, List


class NumOfTherapistMapper:

    def to_weekly_therapists_map(self, dataframe: DataFrame) -> Dict:
        """
        Converts that given `dataframe` into a number of therapist dictionary per organization.
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
