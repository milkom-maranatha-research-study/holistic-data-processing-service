import logging
import pandas as pd
import os

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pandas import DataFrame

from data_processor.src.helpers import print_time_duration


logger = logging.getLogger(__name__)


ORG_ACTIVE_THERS_INPUT_PATH = 'output/org/active-ther-aggregate'
ORG_WEEKLY_ACTIVE_THERS_INPUT_FILENAME = 'org-active-ther-weekly-aggregate'
ORG_MONTHLY_ACTIVE_THERS_INPUT_FILENAME = 'org-active-ther-monthly-aggregate'
ORG_YEARLY_ACTIVE_THERS_INPUT_FILENAME = 'org-active-ther-yearly-aggregate'

ORG_WEEKLY_INPUT_RATE_OUTPUT_PATH = 'input/org-rate/weekly'
ORG_WEEKLY_INPUT_RATE_OUTPUT_FILENAME = 'input-rate-weekly'

ORG_MONTHLY_INPUT_RATE_OUTPUT_PATH = 'input/org-rate/monthly'
ORG_MONTHLY_INPUT_RATE_OUTPUT_FILENAME = 'input-rate-monthly'

ORG_YEARLY_INPUT_RATE_OUTPUT_PATH = 'input/org-rate/yearly'
ORG_YEARLY_INPUT_RATE_OUTPUT_FILENAME = 'input-rate-yearly'


class OrgActiveTherProcessor:

    def _calculate_num_of_thers_before_period(self, dataframe: DataFrame) -> DataFrame:
        """
        Calculate total active/inactive ther before period per Organization
        and returns a copy of that `dataframe`.
        """
        organization_ids = dataframe['organization_id'].drop_duplicates().array

        dfs = []

        for org_id in organization_ids:
            org_df = dataframe[dataframe['organization_id'] == org_id]

            org_df[[
                'active_ther_b_period', 'inactive_ther_b_period'
            ]] = org_df[['active_ther', 'inactive_ther']].shift(1)

            org_df = org_df.fillna(0).astype(int)

            dfs.append(org_df)

        dataframe = pd.concat(dfs)

        # Period start/end somehow is converted to milliseconds
        # Therefore we need to convert them back to `datetime` object.
        dataframe[['period_start', 'period_end']] = dataframe[
            ['period_start', 'period_end']
        ].apply(pd.to_datetime, errors='coerce')

        return dataframe

    def _to_csv(self, dataframe: DataFrame, path: str, filename: str) -> None:
        """
        Saves that `dataframe` into CSV files.
        """
        is_exists = os.path.exists(path)

        if not is_exists:
            os.makedirs(path)

        logger.info("Save Organizations' active therapists data into CSV file...")

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
            f'{path}/{filename}.csv',
            index=False,
            header=False
        )


class OrgWeeklyActiveTherProcessor(OrgActiveTherProcessor):

    def __init__(self) -> None:

        # Runs active thers data processor
        process_start_at = datetime.now()

        self._process_data()

        process_end_at = datetime.now()

        tag = "Organizations' active therapists weekly data processing"
        print_time_duration(tag, process_start_at, process_end_at)

    def _process_data(self) -> None:
        """
        Process Organizations' weekly active therapists data
        and writes the result into multiple CSV files.
        """

        logger.info("Load Organizations' weekly active therapists data from disk...")

        # Step 1 - Load weekly active therapists
        dataframe = pd.read_csv(
            f'{ORG_ACTIVE_THERS_INPUT_PATH}/{ORG_WEEKLY_ACTIVE_THERS_INPUT_FILENAME}.csv',
            sep='\t',
            names=['period', 'organization_id', 'active_ther', 'inactive_ther', 'total_ther'],
            dtype={
                'period': 'str',
                'organization_id': 'Int64',
                'active_ther': 'Int64',
                'inactive_ther': 'Int64',
                'total_ther': 'Int64'
            }
        )

        logger.info("Processing dataframe...")

        # Step 2 - Extracts `period_start` and `period_end` columns
        # * 2.1 Split period start/end from the `period` column
        dataframe[['period_start', 'period_end']] = dataframe.period.str.split('/', expand=True)

        # * 2.2 Convert `period_start` and `period_end` columns into datetime object
        dataframe[['period_start', 'period_end']] = dataframe[
            ['period_start', 'period_end']
        ].apply(pd.to_datetime, yearfirst=True, errors='coerce')

        # Step 3 - Removes unnecessary columns
        dataframe = dataframe.drop(columns=['period'])

        # Step 4 - Sort dataframe by date period
        dataframe = dataframe.sort_values(by=['period_start', 'period_end']).reset_index(drop=True)

        logger.info("Calculating active/inactive thers before period per Organization...")

        # Step 5 - Calculate active/inactive thers before period
        dataframe = self._calculate_num_of_thers_before_period(dataframe)

        # Step 6 - Save results into CSV file
        self._to_csv(dataframe, ORG_WEEKLY_INPUT_RATE_OUTPUT_PATH, ORG_WEEKLY_INPUT_RATE_OUTPUT_FILENAME)


class OrgMonthlyActiveTherProcessor(OrgActiveTherProcessor):

    def __init__(self) -> None:

        # Runs active thers data processor
        process_start_at = datetime.now()

        self._process_data()

        process_end_at = datetime.now()

        tag = "Organizations' active therapists monthly data processing"
        print_time_duration(tag, process_start_at, process_end_at)

    def _process_data(self) -> None:
        """
        Process Organizations' monthly active therapists data
        and writes the result into multiple CSV files.
        """

        logger.info("Load Organizations' monthly active therapists data from disk...")

        # Step 1 - Load Organizations' monthly active therapists
        dataframe = pd.read_csv(
            f'{ORG_ACTIVE_THERS_INPUT_PATH}/{ORG_MONTHLY_ACTIVE_THERS_INPUT_FILENAME}.csv',
            sep='\t',
            names=['period', 'organization_id', 'active_ther', 'inactive_ther', 'total_ther'],
            dtype={
                'period': 'str',
                'organization_id': 'Int64',
                'active_ther': 'Int64',
                'inactive_ther': 'Int64',
                'total_ther': 'Int64'
            }
        )

        logger.info("Processing dataframe...")

        # Step 2 - Generates `period_start` and `period_end` columns
        dataframe = self._generate_date_period(dataframe)

        # Step 3 - Removes unnecessary columns
        dataframe = dataframe.drop(columns=['period'])

        # Step 4 - Sort dataframe by date period
        dataframe = dataframe.sort_values(by=['period_start', 'period_end']).reset_index(drop=True)

        logger.info("Calculating active/inactive thers before period per Organization...")

        # Step 5 - Calculate active/inactive thers before period
        dataframe = self._calculate_num_of_thers_before_period(dataframe)

        # Step 6 - Save results into CSV file
        self._to_csv(dataframe, ORG_MONTHLY_INPUT_RATE_OUTPUT_PATH, ORG_MONTHLY_INPUT_RATE_OUTPUT_FILENAME)

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


class OrgYearlyActiveTherProcessor(OrgActiveTherProcessor):

    def __init__(self) -> None:

        # Runs active thers data processor
        process_start_at = datetime.now()

        self._process_data()

        process_end_at = datetime.now()

        tag = "Active Organizations' therapists yearly data processing"
        print_time_duration(tag, process_start_at, process_end_at)

    def _process_data(self) -> None:
        """
        Process Organizations' yearly active therapists data
        and writes the result into multiple CSV files.
        """

        # Step 1 - Load yearly active therapists
        logger.info("Load Organizations' yearly active therapists data from disk...")
        dataframe = pd.read_csv(
            f'{ORG_ACTIVE_THERS_INPUT_PATH}/{ORG_YEARLY_ACTIVE_THERS_INPUT_FILENAME}.csv',
            sep='\t',
            names=['period', 'organization_id', 'active_ther', 'inactive_ther', 'total_ther'],
            dtype={
                'period': 'str',
                'organization_id': 'Int64',
                'active_ther': 'Int64',
                'inactive_ther': 'Int64',
                'total_ther': 'Int64'
            }
        )

        logger.info("Processing dataframe...")

        # Step 2 - Generates `period_start` and `period_end` columns
        dataframe = self._generate_date_period(dataframe)

        # Step 3 - Removes unnecessary columns
        dataframe = dataframe.drop(columns=['period'])

        # Step 4 - Sort dataframe by date period
        dataframe = dataframe.sort_values(by=['period_start', 'period_end']).reset_index(drop=True)

        logger.info("Calculating active/inactive thers before period per Organization...")

        # Step 5 - Calculate active/inactive thers before period
        dataframe = self._calculate_num_of_thers_before_period(dataframe)

        # Step 6 - Save results into CSV file
        self._to_csv(dataframe, ORG_YEARLY_INPUT_RATE_OUTPUT_PATH, ORG_YEARLY_INPUT_RATE_OUTPUT_FILENAME)

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
            lambda row: row['period_start'] + relativedelta(day=31, month=12),
            axis=1
        )

        return dataframe
