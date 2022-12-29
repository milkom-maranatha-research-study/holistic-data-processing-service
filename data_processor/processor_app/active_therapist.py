import logging
import pandas as pd
import os

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pandas import DataFrame

from data_processor.src.helpers import print_time_duration
from data_processor.settings import configure_logging


logger = logging.getLogger(__name__)


AGG_ACTIVE_THERS_INPUT_PATH = 'output/active-inactive'
AGG_ACTIVE_THERS_FILENAME = 'aggregate'

ORG_RATE_INPUT_PATH = 'input/organization_rate'
ORG_RATE_FILENAME = 'rate'


class WeeklyActiveTherProcessor:

    def __init__(self) -> None:

        # Runs active thers data processor
        process_start_at = datetime.now()

        self._process_data()

        process_end_at = datetime.now()

        tag = "Active therapists weekly data processing"
        print_time_duration(tag, process_start_at, process_end_at)

    def _process_data(self) -> None:
        """
        Normalize weekly active therapists
        and writes the result into multiple CSV files.
        """

        # Step 1 - Load weekly active therapists
        logger.info("Load weekly active therapists data from disk...")
        dataframe = pd.read_csv(
            f'{AGG_ACTIVE_THERS_INPUT_PATH}/weekly-{AGG_ACTIVE_THERS_FILENAME}.csv',
            sep='\t',
            names=['period_org_id', 'active_inactive', 'total_ther'],
            dtype={
                'period_org_id': 'str',
                'active_inactive': 'str',
                'total_ther': 'Int64'
            }
        )

        # Step 2 - Normalize dataframe
        logger.info("Processing dataframe...")

        # * 2.1 Extract `period_start`, `period_end`, and `organization_id` columns
        dataframe[['period_start', 'period_end_org_id']] = dataframe.period_org_id.str.split('/', expand=True)
        dataframe[['period_end', 'organization_id']] = dataframe.period_end_org_id.str.split(',', expand=True)

        # * 2.2 Convert `period_start` and `period_end` columns into datetime object
        dataframe[['period_start', 'period_end']] = dataframe[
            ['period_start', 'period_end']
        ].apply(pd.to_datetime, errors='coerce')

        # * 2.3 Extract `active_ther` and `inactive_ther` columns
        dataframe[['active_ther', 'inactive_ther']] = dataframe.active_inactive.str.split(',', expand=True)

        # * 2.4 Convert `organization_id`, `active_ther` and `inactive_ther` columns data type into a number
        dataframe[['organization_id', 'active_ther', 'inactive_ther']] = dataframe[
            ['organization_id', 'active_ther', 'inactive_ther']
        ].apply(pd.to_numeric, errors='coerce')

        # * 2.5 Removes unnecessary columns
        dataframe = dataframe.drop(columns=['active_inactive', 'period_org_id', 'period_end_org_id'])

        # Step 3 - Sort dataframe by date period
        logger.info("Sorting dataframe by date period...")
        dataframe = dataframe.sort_values(by=['period_start', 'period_end']).reset_index(drop=True)

        # Step 4 - Calculate active/inactive thers before period
        logger.info("Calculating active/inactive thers before period...")
        dataframe = self._calculate_num_of_thers_before_period(dataframe)

        # Step 5 - Save results into CSV files
        self._to_csv(dataframe)

    def _calculate_num_of_thers_before_period(self, dataframe: DataFrame) -> DataFrame:
        """
        Calculate total active/inactive ther before period per Organization
        and returns a copy of that `dataframe`.
        """
        organization_ids = dataframe['organization_id'].drop_duplicates().array

        joined_df = None

        for org_id in organization_ids:
            org_df = dataframe[dataframe['organization_id'] == org_id]

            org_df[[
                'active_ther_b_period', 'inactive_ther_b_period'
            ]] = org_df[['active_ther', 'inactive_ther']].shift(1)

            org_df = org_df.fillna(0).astype(int)

            if joined_df is None:
                joined_df = org_df
            else:
                joined_df = pd.concat([joined_df, org_df])

        joined_df[['period_start', 'period_end']] = joined_df[
            ['period_start', 'period_end']
        ].apply(pd.to_datetime, errors='coerce')

        return joined_df

    def _to_csv(self, dataframe: DataFrame) -> None:
        """
        Saves that `dataframe` into CSV files.
        """
        is_exists = os.path.exists(ORG_RATE_INPUT_PATH)

        if not is_exists:
            os.makedirs(ORG_RATE_INPUT_PATH)

        logger.info("Save Therapist data into CSV files...")

        dataframe[[
            'period_start',
            'period_end',
            'organization_id',
            'active_ther',
            'inactive_ther',
            'total_ther',
            'active_ther_b_period',
            'inactive_ther_b_period'
        ]].to_csv(
            f'{ORG_RATE_INPUT_PATH}/weekly-per-org-{ORG_RATE_FILENAME}.csv',
            index=False,
            header=False
        )


class MonthlyActiveTherProcessor:

    def __init__(self) -> None:

        # Runs active thers data processor
        process_start_at = datetime.now()

        self._process_data()

        process_end_at = datetime.now()

        tag = "Active therapists monthly data processing"
        print_time_duration(tag, process_start_at, process_end_at)

    def _process_data(self) -> None:
        """
        Normalize monthly active therapists
        and writes the result into multiple CSV files.
        """

        # Step 1 - Load monthly active therapists
        logger.info("Load monthly active therapists data from disk...")
        dataframe = pd.read_csv(
            f'{AGG_ACTIVE_THERS_INPUT_PATH}/monthly-{AGG_ACTIVE_THERS_FILENAME}.csv',
            sep='\t',
            names=['period_org_id', 'active_inactive', 'total_ther'],
            dtype={
                'period_org_id': 'str',
                'active_inactive': 'str',
                'total_ther': 'Int64'
            }
        )

        # Step 2 - Normalize dataframe
        logger.info("Processing dataframe...")

        # * 2.1 Extract `period` and `organization_id` columns
        dataframe[['period', 'organization_id']] = dataframe.period_org_id.str.split(',', expand=True)

        # * 2.2 Generates `period_start` and `period_end` columns
        dataframe = self._generate_date_period(dataframe)

        # * 2.3 Extract `active_ther` and `inactive_ther` columns
        dataframe[['active_ther', 'inactive_ther']] = dataframe.active_inactive.str.split(',', expand=True)

        # * 2.4 Convert `organization_id`, `active_ther` and `inactive_ther` columns data type into a number
        dataframe[['organization_id', 'active_ther', 'inactive_ther']] = dataframe[
            ['organization_id', 'active_ther', 'inactive_ther']
        ].apply(pd.to_numeric, errors='coerce')

        # * 2.5 Removes unnecessary columns
        dataframe = dataframe.drop(columns=['active_inactive', 'period_org_id', 'period'])

        # Step 3 - Sort dataframe by date period
        logger.info("Sorting dataframe by date period...")
        dataframe = dataframe.sort_values(by=['period_start', 'period_end']).reset_index(drop=True)

        # Step 4 - Calculate active/inactive thers before period
        logger.info("Calculating active/inactive thers before period...")
        dataframe = self._calculate_num_of_thers_before_period(dataframe)

        # Step 5 - Save results into CSV files
        self._to_csv(dataframe)

    def _generate_date_period(self, dataframe: DataFrame) -> DataFrame:
        """
        Generates date period on that `dataframe`.
        """
        # Generates `period_start` column
        dataframe['period_start'] = dataframe.apply(lambda row: row['period'] + '-01', axis=1)

        # Converts `period_start` column data type to a `datetime`
        dataframe['period_start'] = dataframe[['period_start']].apply(pd.to_datetime, errors='coerce')

        # Generates `period_end` column
        dataframe['period_end'] = dataframe.apply(
            lambda row: row['period_start'] + relativedelta(day=31),
            axis=1
        )

        return dataframe

    def _calculate_num_of_thers_before_period(self, dataframe: DataFrame) -> DataFrame:
        """
        Calculates total active/inactive ther before period per Organization
        and returns a copy of that `dataframe`.
        """
        organization_ids = dataframe['organization_id'].drop_duplicates().array

        joined_df = None

        for org_id in organization_ids:
            org_df = dataframe[dataframe['organization_id'] == org_id]

            org_df[[
                'active_ther_b_period', 'inactive_ther_b_period'
            ]] = org_df[['active_ther', 'inactive_ther']].shift(1)

            org_df = org_df.fillna(0).astype(int)

            if joined_df is None:
                joined_df = org_df
            else:
                joined_df = pd.concat([joined_df, org_df])

        joined_df[['period_start', 'period_end']] = joined_df[
            ['period_start', 'period_end']
        ].apply(pd.to_datetime, errors='coerce')

        return joined_df

    def _to_csv(self, dataframe: DataFrame) -> None:
        """
        Saves that `dataframe` into CSV files.
        """
        is_exists = os.path.exists(ORG_RATE_INPUT_PATH)

        if not is_exists:
            os.makedirs(ORG_RATE_INPUT_PATH)

        logger.info("Save Therapist data into CSV files...")

        dataframe[[
            'period_start',
            'period_end',
            'organization_id',
            'active_ther',
            'inactive_ther',
            'total_ther',
            'active_ther_b_period',
            'inactive_ther_b_period'
        ]].to_csv(
            f'{ORG_RATE_INPUT_PATH}/monthly-per-org-{ORG_RATE_FILENAME}.csv',
            index=False,
            header=False
        )


class YearlyActiveTherProcessor:

    def __init__(self) -> None:

        # Runs active thers data processor
        process_start_at = datetime.now()

        self._process_data()

        process_end_at = datetime.now()

        tag = "Active therapists yearly data processing"
        print_time_duration(tag, process_start_at, process_end_at)

    def _process_data(self) -> None:
        """
        Normalize yearly active therapists
        and writes the result into multiple CSV files.
        """

        # Step 1 - Load yearly active therapists
        logger.info("Load yearly active therapists data from disk...")
        dataframe = pd.read_csv(
            f'{AGG_ACTIVE_THERS_INPUT_PATH}/yearly-{AGG_ACTIVE_THERS_FILENAME}.csv',
            sep='\t',
            names=['period_org_id', 'active_inactive', 'total_ther'],
            dtype={
                'period_org_id': 'str',
                'active_inactive': 'str',
                'total_ther': 'Int64'
            }
        )

        # Step 2 - Normalize dataframe
        logger.info("Processing dataframe...")

        # * 2.1 Extract `period` and `organization_id` columns
        dataframe[['period', 'organization_id']] = dataframe.period_org_id.str.split(',', expand=True)

        # * 2.2 Generates `period_start` and `period_end` columns
        dataframe = self._generate_date_period(dataframe)

        # * 2.3 Extract `active_ther` and `inactive_ther` columns
        dataframe[['active_ther', 'inactive_ther']] = dataframe.active_inactive.str.split(',', expand=True)

        # * 2.4 Convert `organization_id`, `active_ther` and `inactive_ther` columns data type into a number
        dataframe[['organization_id', 'active_ther', 'inactive_ther']] = dataframe[
            ['organization_id', 'active_ther', 'inactive_ther']
        ].apply(pd.to_numeric, errors='coerce')

        # * 2.5 Removes unnecessary columns
        dataframe = dataframe.drop(columns=['active_inactive', 'period_org_id', 'period'])

        # Step 3 - Sort dataframe by date period
        logger.info("Sorting dataframe by date period...")
        dataframe = dataframe.sort_values(by=['period_start', 'period_end']).reset_index(drop=True)

        # Step 4 - Calculate active/inactive thers before period
        logger.info("Calculating active/inactive thers before period...")
        dataframe = self._calculate_num_of_thers_before_period(dataframe)
        from pdb import set_trace
        set_trace()
        # Step 5 - Save results into CSV files
        self._to_csv(dataframe)

    def _generate_date_period(self, dataframe: DataFrame) -> DataFrame:
        """
        Generates date period on that `dataframe`.
        """
        # Generates `period_start` column
        dataframe['period_start'] = dataframe.apply(lambda row: row['period'] + '-01-01', axis=1)

        # Converts `period_start` column data type to a `datetime`
        dataframe['period_start'] = dataframe[['period_start']].apply(pd.to_datetime, errors='coerce')

        # Generates `period_end` column
        dataframe['period_end'] = dataframe.apply(
            lambda row: row['period_start'] + relativedelta(day=31, month=11),
            axis=1
        )

        return dataframe

    def _calculate_num_of_thers_before_period(self, dataframe: DataFrame) -> DataFrame:
        """
        Calculates total active/inactive ther before period per Organization
        and returns a copy of that `dataframe`.
        """
        organization_ids = dataframe['organization_id'].drop_duplicates().array

        joined_df = None

        for org_id in organization_ids:
            org_df = dataframe[dataframe['organization_id'] == org_id]

            org_df[[
                'active_ther_b_period', 'inactive_ther_b_period'
            ]] = org_df[['active_ther', 'inactive_ther']].shift(1)

            org_df = org_df.fillna(0).astype(int)

            if joined_df is None:
                joined_df = org_df
            else:
                joined_df = pd.concat([joined_df, org_df])

        joined_df[['period_start', 'period_end']] = joined_df[
            ['period_start', 'period_end']
        ].apply(pd.to_datetime, errors='coerce')

        return joined_df

    def _to_csv(self, dataframe: DataFrame) -> None:
        """
        Saves that `dataframe` into CSV files.
        """
        is_exists = os.path.exists(ORG_RATE_INPUT_PATH)

        if not is_exists:
            os.makedirs(ORG_RATE_INPUT_PATH)

        logger.info("Save Therapist data into CSV files...")

        dataframe[[
            'period_start',
            'period_end',
            'organization_id',
            'active_ther',
            'inactive_ther',
            'total_ther',
            'active_ther_b_period',
            'inactive_ther_b_period'
        ]].to_csv(
            f'{ORG_RATE_INPUT_PATH}/yearly-per-org-{ORG_RATE_FILENAME}.csv',
            index=False,
            header=False
        )


if __name__ == '__main__':
    configure_logging()

    WeeklyActiveTherProcessor()
    MonthlyActiveTherProcessor()
    YearlyActiveTherProcessor()
