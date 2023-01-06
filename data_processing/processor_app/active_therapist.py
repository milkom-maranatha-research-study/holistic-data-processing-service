import logging
import pandas as pd
import os

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pandas import DataFrame
from typing import List

from data_processing.settings import configure_logging
from data_processing.src.dateutil import (
    is_one_week_diff,
    is_one_month_diff,
    is_one_year_diff,
)
from data_processing.src.helpers import print_time_duration


logger = logging.getLogger(__name__)


ORG_DIR = 'by-org'
APP_DIR = 'by-app'

INPUT_ACTIVE_THER_PATH = 'output/active-ther'

WEEKLY_ACTIVE_THER_INPUT_FILENAME = 'active-ther-weekly-aggregate'
MONTHLY_ACTIVE_THER_INPUT_FILENAME = 'active-ther-monthly-aggregate'
YEARLY_ACTIVE_THER_INPUT_FILENAME = 'active-ther-yearly-aggregate'

INPUT_RATE_OUTPUT_PATH = 'input/rate'

WEEKLY_INPUT_RATE_OUTPUT_FILENAME = 'input-rate-weekly'
MONTHLY_INPUT_RATE_OUTPUT_FILENAME = 'input-rate-monthly'
YEARLY_INPUT_RATE_OUTPUT_FILENAME = 'input-rate-yearly'


class OrgActiveTherProcessor:

    def set_total_thers_before_period(
        self,
        dataframe: DataFrame,
        period_type: str
    ) -> DataFrame:
        """
        Calculate total active/inactive therapists in the Organization
        before particular period is started and returns a copy of that `dataframe`.
        """
        dataframes = self._split_dataframe_per_org(dataframe)

        for df in dataframes:
            self._set_value_before_period(df, period_type)

        # Merge all dataframes into a single dataframe
        dataframe = pd.concat(dataframes).reset_index().drop(columns=['level_0', 'index'])

        return dataframe

    def _split_dataframe_per_org(self, dataframe: DataFrame) -> List[DataFrame]:
        """
        Splits that `dataframe` per organization ID.
        """
        organization_ids = dataframe['organization_id'].drop_duplicates().array

        # We use `.copy()` to avoid `SettingWithCopyWarning`.
        # The warning arises because we make a subset of the main dataframe,
        # but then we modify it immediately.
        # The subset of the main dataframe is created using shallow copy.
        # Therefore the warning tells us that the action might affecting the main
        # dataframe and some inconsistency is expected to occur.
        dataframes = [
            dataframe[dataframe['organization_id'] == org_id].copy(deep=True).reset_index()
            for org_id in organization_ids
        ]

        return dataframes

    def _set_value_before_period(self, dataframe: DataFrame, period_type: str) -> None:
        """
        Set the total active/inactive therapists before period started.
        """
        for index, row in dataframe.iterrows():

            if index == 0:
                self._set_with_zero(index, dataframe)
                continue

            current_period_end = row['period_end']
            prev_period_end = dataframe.loc[index - 1, ['period_end']]['period_end']

            if period_type == 'weekly':
                if is_one_week_diff(prev_period_end, current_period_end):
                    self._set_with_prev_row(index, dataframe)
                    continue

            elif period_type == 'monthly':
                if is_one_month_diff(prev_period_end, current_period_end):
                    self._set_with_prev_row(index, dataframe)
                    continue

            elif period_type == 'yearly':
                if is_one_year_diff(prev_period_end, current_period_end):
                    self._set_with_prev_row(index, dataframe)
                    continue

            self._set_with_zero(index, dataframe)

    def _set_with_zero(self, index, dataframe) -> None:
        """
        Sets the total active/inactive therapists before period is started
        on that current `index` of that `dataframe` to zero.
        """
        dataframe.loc[index, ['active_ther_b_period']] = [0]
        dataframe.loc[index, ['inactive_ther_b_period']] = [0]

    def _set_with_prev_row(self, index, dataframe) -> None:
        """
        Sets the total active/inactive therapists before period is started
        on that current `index` of that `dataframe`
        with the total active/inactive therapists on the previous period.
        """
        prev_active_ther = dataframe.loc[index - 1, ['active_ther']]['active_ther']
        prev_inactive_ther = dataframe.loc[index - 1, ['inactive_ther']]['inactive_ther']

        dataframe.loc[index, ['active_ther_b_period']] = [prev_active_ther]
        dataframe.loc[index, ['inactive_ther_b_period']] = [prev_inactive_ther]


class OrgWeeklyActiveTherProcessor(OrgActiveTherProcessor):

    def __init__(self) -> None:

        # Runs active thers data processor
        process_start_at = datetime.now()

        self._process_data()

        process_end_at = datetime.now()

        tag = "Weekly active therapists per Organization's data processing"
        print_time_duration(tag, process_start_at, process_end_at)

    def _process_data(self) -> None:
        """
        Process weekly active therapists per Organization
        and writes the result into multiple CSV files.
        """

        logger.info("Load weekly active therapists per Organization from disk...")

        # Step 1 - Load weekly active therapists
        path = f'{INPUT_ACTIVE_THER_PATH}/{ORG_DIR}/weekly'

        dataframe = pd.read_csv(
            f'{path}/{WEEKLY_ACTIVE_THER_INPUT_FILENAME}.csv',
            sep='\t',
            names=[
                'period',
                'organization_id',
                'active_ther',
                'inactive_ther',
                'total_ther',
                'active_ther_b_period',
                'inactive_ther_b_period'
            ],
            dtype={
                'period': 'str',
                'organization_id': 'Int64',
                'active_ther': 'Int64',
                'inactive_ther': 'Int64',
                'total_ther': 'Int64',
                'active_ther_b_period': 'Int64',
                'inactive_ther_b_period': 'Int64'
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
        dataframe = self.set_total_thers_before_period(dataframe, 'weekly')

        logger.info("Save active therapists per Organization into CSV file...")

        # Step 6 - Save results into CSV file
        # * Create output directory if it doesn't exists
        output_path = f'{INPUT_RATE_OUTPUT_PATH}/{ORG_DIR}/weekly'

        is_exists = os.path.exists(output_path)

        if not is_exists:
            os.makedirs(output_path)

        # * Export dataframe to CSV file
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
            f'{output_path}/{WEEKLY_INPUT_RATE_OUTPUT_FILENAME}.csv',
            index=False,
            header=False
        )


class OrgMonthlyActiveTherProcessor(OrgActiveTherProcessor):

    def __init__(self) -> None:

        # Runs active thers data processor
        process_start_at = datetime.now()

        self._process_data()

        process_end_at = datetime.now()

        tag = "Monthly active therapists per Organization's data processing"
        print_time_duration(tag, process_start_at, process_end_at)

    def _process_data(self) -> None:
        """
        Process monthly active therapists per Organization
        and writes the result into multiple CSV files.
        """

        logger.info("Load monthly active therapists per Organization from disk...")

        # Step 1 - Load monthly active therapists per Organization
        path = f'{INPUT_ACTIVE_THER_PATH}/{ORG_DIR}/monthly'

        dataframe = pd.read_csv(
            f'{path}/{MONTHLY_ACTIVE_THER_INPUT_FILENAME}.csv',
            sep='\t',
            names=[
                'period',
                'organization_id',
                'active_ther',
                'inactive_ther',
                'total_ther',
                'active_ther_b_period',
                'inactive_ther_b_period'
            ],
            dtype={
                'period': 'str',
                'organization_id': 'Int64',
                'active_ther': 'Int64',
                'inactive_ther': 'Int64',
                'total_ther': 'Int64',
                'active_ther_b_period': 'Int64',
                'inactive_ther_b_period': 'Int64'
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
        dataframe = self.set_total_thers_before_period(dataframe, 'monthly')

        logger.info("Save active therapists per Organization into CSV file...")

        # Step 6 - Save results into CSV file
        # * Create output directory if it doesn't exists
        output_path = f'{INPUT_RATE_OUTPUT_PATH}/{ORG_DIR}/monthly'

        is_exists = os.path.exists(output_path)

        if not is_exists:
            os.makedirs(output_path)

        # * Export dataframe to CSV file
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
            f'{output_path}/{MONTHLY_INPUT_RATE_OUTPUT_FILENAME}.csv',
            index=False,
            header=False
        )

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

        tag = "Yearly active therapists per Organization's data processing"
        print_time_duration(tag, process_start_at, process_end_at)

    def _process_data(self) -> None:
        """
        Process yearly active therapists per Organization
        and writes the result into multiple CSV files.
        """

        # Step 1 - Load yearly active therapists
        logger.info("Load data yearly active therapists per Organization from disk...")
        path = f'{INPUT_ACTIVE_THER_PATH}/{ORG_DIR}/yearly'

        dataframe = pd.read_csv(
            f'{path}/{YEARLY_ACTIVE_THER_INPUT_FILENAME}.csv',
            sep='\t',
            names=[
                'period',
                'organization_id',
                'active_ther',
                'inactive_ther',
                'total_ther',
                'active_ther_b_period',
                'inactive_ther_b_period'
            ],
            dtype={
                'period': 'str',
                'organization_id': 'Int64',
                'active_ther': 'Int64',
                'inactive_ther': 'Int64',
                'total_ther': 'Int64',
                'active_ther_b_period': 'Int64',
                'inactive_ther_b_period': 'Int64'
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
        dataframe = self.set_total_thers_before_period(dataframe, 'yearly')

        logger.info("Save active therapists per Organization into CSV file...")

        # Step 6 - Save results into CSV file
        # * Create output directory if it doesn't exists
        output_path = f'{INPUT_RATE_OUTPUT_PATH}/{ORG_DIR}/yearly'

        is_exists = os.path.exists(output_path)

        if not is_exists:
            os.makedirs(output_path)

        # * Export dataframe to CSV file
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
            f'{output_path}/{YEARLY_INPUT_RATE_OUTPUT_FILENAME}.csv',
            index=False,
            header=False
        )

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


class NDActiveTherProcessor:

    def set_total_thers_before_period(
        self,
        dataframe: DataFrame,
        period_type: str
    ) -> DataFrame:
        """
        Calculate total active/inactive therapists in NiceDay before particular period
        is started and returns a copy of that `dataframe`.
        """

        for index, row in dataframe.iterrows():

            if index == 0:
                self._set_with_zero(index, dataframe)
                continue

            current_period_end = row['period_end']
            prev_period_end = dataframe.loc[index - 1, ['period_end']]['period_end']

            if period_type == 'weekly':
                if is_one_week_diff(prev_period_end, current_period_end):
                    self._set_with_prev_row(index, dataframe)
                    continue

            elif period_type == 'monthly':
                if is_one_month_diff(prev_period_end, current_period_end):
                    self._set_with_prev_row(index, dataframe)
                    continue

            elif period_type == 'yearly':
                if is_one_year_diff(prev_period_end, current_period_end):
                    self._set_with_prev_row(index, dataframe)
                    continue

            self._set_with_zero(index, dataframe)

        return dataframe

    def _set_with_zero(self, index, dataframe) -> None:
        """
        Sets the total active/inactive therapists before period is started
        on that current `index` of that `dataframe` to zero.
        """
        dataframe.loc[index, ['active_ther_b_period']] = [0]
        dataframe.loc[index, ['inactive_ther_b_period']] = [0]

    def _set_with_prev_row(self, index, dataframe) -> None:
        """
        Sets the total active/inactive therapists before period is started
        on that current `index` of that `dataframe`
        with the total active/inactive therapists on the previous period.
        """
        prev_active_ther = dataframe.loc[index - 1, ['active_ther']]['active_ther']
        prev_inactive_ther = dataframe.loc[index - 1, ['inactive_ther']]['inactive_ther']

        dataframe.loc[index, ['active_ther_b_period']] = [prev_active_ther]
        dataframe.loc[index, ['inactive_ther_b_period']] = [prev_inactive_ther]


class NDWeeklyActiveTherProcessor(NDActiveTherProcessor):

    def __init__(self) -> None:

        # Runs active thers data processor
        process_start_at = datetime.now()

        self._process_data()

        process_end_at = datetime.now()

        tag = "Weekly active therapists in NiceDay's data processing"
        print_time_duration(tag, process_start_at, process_end_at)

    def _process_data(self) -> None:
        """
        Process weekly active therapists in NiceDay
        and writes the result into multiple CSV files.
        """

        logger.info("Load weekly active therapists in NiceDay from disk...")

        # Step 1 - Load weekly active therapists
        path = f'{INPUT_ACTIVE_THER_PATH}/{APP_DIR}/weekly'

        dataframe = pd.read_csv(
            f'{path}/{WEEKLY_ACTIVE_THER_INPUT_FILENAME}.csv',
            sep='\t',
            names=[
                'period',
                'active_ther',
                'inactive_ther',
                'total_ther',
                'active_ther_b_period',
                'inactive_ther_b_period'
            ],
            dtype={
                'period': 'str',
                'active_ther': 'Int64',
                'inactive_ther': 'Int64',
                'total_ther': 'Int64',
                'active_ther_b_period': 'Int64',
                'inactive_ther_b_period': 'Int64'
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
        dataframe = self.set_total_thers_before_period(dataframe, 'weekly')

        logger.info("Save active therapists data in NiceDay into CSV file...")

        # Step 6 - Save results into CSV file
        # * Create output directory if it doesn't exists
        output_path = f'{INPUT_RATE_OUTPUT_PATH}/{APP_DIR}/weekly'

        is_exists = os.path.exists(output_path)

        if not is_exists:
            os.makedirs(output_path)

        # * Export dataframe to CSV file
        dataframe[[
            'period_start',
            'period_end',
            'active_ther',
            'inactive_ther',
            'total_ther',
            'active_ther_b_period',
            'inactive_ther_b_period'
        ]].to_csv(
            f'{output_path}/{WEEKLY_INPUT_RATE_OUTPUT_FILENAME}.csv',
            index=False,
            header=False
        )


class NDMonthlyActiveTherProcessor(NDActiveTherProcessor):

    def __init__(self) -> None:

        # Runs active thers data processor
        process_start_at = datetime.now()

        self._process_data()

        process_end_at = datetime.now()

        tag = "Monthly active therapists in NiceDay's data processing"
        print_time_duration(tag, process_start_at, process_end_at)

    def _process_data(self) -> None:
        """
        Process monthly active therapists in NiceDay
        and writes the result into multiple CSV files.
        """

        logger.info("Load monthly active therapists in NiceDay from disk...")

        # Step 1 - Load monthly active therapists
        path = f'{INPUT_ACTIVE_THER_PATH}/{APP_DIR}/monthly'

        dataframe = pd.read_csv(
            f'{path}/{MONTHLY_ACTIVE_THER_INPUT_FILENAME}.csv',
            sep='\t',
            names=[
                'period',
                'active_ther',
                'inactive_ther',
                'total_ther',
                'active_ther_b_period',
                'inactive_ther_b_period'
            ],
            dtype={
                'period': 'str',
                'active_ther': 'Int64',
                'inactive_ther': 'Int64',
                'total_ther': 'Int64',
                'active_ther_b_period': 'Int64',
                'inactive_ther_b_period': 'Int64'
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
        dataframe = self.set_total_thers_before_period(dataframe, 'monthly')

        logger.info("Save active therapists data in NiceDay into CSV file...")

        # Step 6 - Save results into CSV file
        # * Create output directory if it doesn't exists
        output_path = f'{INPUT_RATE_OUTPUT_PATH}/{APP_DIR}/monthly'

        is_exists = os.path.exists(output_path)

        if not is_exists:
            os.makedirs(output_path)

        # * Export dataframe to CSV file
        dataframe[[
            'period_start',
            'period_end',
            'active_ther',
            'inactive_ther',
            'total_ther',
            'active_ther_b_period',
            'inactive_ther_b_period'
        ]].to_csv(
            f'{output_path}/{MONTHLY_INPUT_RATE_OUTPUT_FILENAME}.csv',
            index=False,
            header=False
        )

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


class NDYearlyActiveTherProcessor(NDActiveTherProcessor):

    def __init__(self) -> None:

        # Runs active thers data processor
        process_start_at = datetime.now()

        self._process_data()

        process_end_at = datetime.now()

        tag = "Yearly active therapists in NiceDay's data processing"
        print_time_duration(tag, process_start_at, process_end_at)

    def _process_data(self) -> None:
        """
        Process yearly active therapists in NiceDay
        and writes the result into multiple CSV files.
        """

        # Step 1 - Load yearly active therapists
        logger.info("Load yearly active therapists in NiceDay from disk...")
        path = f'{INPUT_ACTIVE_THER_PATH}/{APP_DIR}/yearly'

        dataframe = pd.read_csv(
            f'{path}/{YEARLY_ACTIVE_THER_INPUT_FILENAME}.csv',
            sep='\t',
            names=[
                'period',
                'active_ther',
                'inactive_ther',
                'total_ther',
                'active_ther_b_period',
                'inactive_ther_b_period'
            ],
            dtype={
                'period': 'str',
                'active_ther': 'Int64',
                'inactive_ther': 'Int64',
                'total_ther': 'Int64',
                'active_ther_b_period': 'Int64',
                'inactive_ther_b_period': 'Int64'
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
        dataframe = self.set_total_thers_before_period(dataframe, 'yearly')

        logger.info("Save active therapists data in NiceDay into CSV file...")

        # Step 6 - Save results into CSV file
        # * Create output directory if it doesn't exists
        output_path = f'{INPUT_RATE_OUTPUT_PATH}/{APP_DIR}/yearly'

        is_exists = os.path.exists(output_path)

        if not is_exists:
            os.makedirs(output_path)

        # * Export dataframe to CSV file
        dataframe[[
            'period_start',
            'period_end',
            'active_ther',
            'inactive_ther',
            'total_ther',
            'active_ther_b_period',
            'inactive_ther_b_period'
        ]].to_csv(
            f'{output_path}/{YEARLY_INPUT_RATE_OUTPUT_FILENAME}.csv',
            index=False,
            header=False
        )

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


class ActiveTherapistProcessor:

    def __init__(self) -> None:

        # Runs active therapists' data processor
        process_start_at = datetime.now()

        OrgWeeklyActiveTherProcessor()
        OrgMonthlyActiveTherProcessor()
        OrgYearlyActiveTherProcessor()

        NDWeeklyActiveTherProcessor()
        NDMonthlyActiveTherProcessor()
        NDYearlyActiveTherProcessor()

        process_end_at = datetime.now()

        tag = "Processing active therapists"
        print_time_duration(tag, process_start_at, process_end_at)


if __name__ == '__main__':
    configure_logging()

    ActiveTherapistProcessor()
