import logging
import os
import pandas as pd

from datetime import datetime
from pandas import DataFrame

from data_processor.processor_app.active_therapist import (
    OrgWeeklyActiveTherProcessor,
    OrgMonthlyActiveTherProcessor,
    OrgYearlyActiveTherProcessor,
)
from data_processor.src.helpers import print_time_duration
from data_processor.settings import configure_logging


logger = logging.getLogger(__name__)


ORG_RATE_DIR = 'by-org'

INPUT_RATE_PATH = 'input/rate'

WEEKLY_RATE_INPUT_FILENAME = 'input-rate-weekly'
MONTHLY_RATE_INPUT_FILENAME = 'input-rate-monthly'
YEARLY_RATE_INPUT_FILENAME = 'input-rate-yearly'

OUTPUT_RATE_PATH = 'output/rate'

WEEKLY_RATE_OUTPUT_FILENAME = 'output-weekly-rate'
MONTHLY_RATE_OUTPUT_FILENAME = 'output-monthly-rate'
YEARLY_RATE_OUTPUT_FILENAME = 'output-yearly-rate'


class ActiveTherapistRate:

    def _get_churn_rate(
        self,
        active_ther_b_period: int,
        active_ther_w_period: int
    ) -> float:
        """
        Returns churn rate value.

        Params:
        - `active_ther_b_period` : Total active therapists before period
        - `active_ther_w_period` : Total active therapists within period
        """
        if active_ther_b_period == 0:
            return 0

        rate = (active_ther_b_period - active_ther_w_period) / active_ther_b_period

        return round(rate, 2)

    def _get_retention_rate(
        self,
        active_ther_b_period: int,
        active_ther_w_period: int
    ) -> float:
        """
        Returns retention rate value.

        Params:
        - `active_ther_b_period` : Total active therapists before period
        - `active_ther_w_period` : Total active therapists within period
        """
        if active_ther_b_period == 0:
            return 0

        rate = active_ther_w_period / active_ther_b_period

        return round(rate, 2)

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
            'churn_rate',
            'retention_rate',
        ]].to_csv(
            f'{path}/{filename}.csv',
            index=False,
            header=False
        )


class OrgWeeklyActiveTherapistRate(ActiveTherapistRate):

    def __init__(self) -> None:

        # Runs data processor
        process_start_at = datetime.now()

        self._calculate()

        process_end_at = datetime.now()

        tag = "Calculating weekly rate of the active therapists per Organization"
        print_time_duration(tag, process_start_at, process_end_at)

    def _calculate(self) -> None:
        """
        Calculate rate of the active therapists per Organization
        and writes the result into CSV file.
        """
        logger.info("Load Organizations' weekly active therapists data from disk...")

        # Step 1 - Load Organizations' weekly active therapists
        path = f'{INPUT_RATE_PATH}/{ORG_RATE_DIR}/weekly'

        dataframe = pd.read_csv(
            f'{path}/{WEEKLY_RATE_INPUT_FILENAME}.csv',
            sep=',',
            names=[
                'period_start',
                'period_end',
                'organization_id',
                'active_ther',
                'inactive_ther',
                'total_ther',
                'active_ther_b_period',
                'inactive_ther_b_period'
            ],
            dtype={
                'period_start': 'str',
                'period_end': 'str',
                'organization_id': 'Int64',
                'active_ther': 'Int64',
                'inactive_ther': 'Int64',
                'total_ther': 'Int64'
            },
            parse_dates=['period_start', 'period_end']
        )

        logger.info("Calculating weekly rate of the active therapists per Organization...")

        # Step 1 - Generate `churn_rate` column
        dataframe['churn_rate'] = dataframe.apply(
            lambda row: self._get_churn_rate(row['active_ther_b_period'], row['active_ther']),
            axis=1
        )

        # Step 2 - Generate `retention_rate` column
        dataframe['retention_rate'] = dataframe.apply(
            lambda row: self._get_retention_rate(row['active_ther_b_period'], row['active_ther']),
            axis=1
        )

        # Step 3 - Save results into CSV file
        output_path = f'{OUTPUT_RATE_PATH}/{ORG_RATE_DIR}/weekly'
        self._to_csv(dataframe, output_path, WEEKLY_RATE_OUTPUT_FILENAME)


class OrgMonthlyActiveTherapistRate(ActiveTherapistRate):

    def __init__(self) -> None:

        # Runs data processor
        process_start_at = datetime.now()

        self._calculate()

        process_end_at = datetime.now()

        tag = "Calculating monthly rate of the active therapists per Organization"
        print_time_duration(tag, process_start_at, process_end_at)

    def _calculate(self) -> None:
        """
        Calculate rate of the active therapists per Organization
        and writes the result into CSV file.
        """
        logger.info("Load Organizations' monthly active therapists data from disk...")

        # Step 1 - Load Organizations' monthly active therapists
        path = f'{INPUT_RATE_PATH}/{ORG_RATE_DIR}/monthly'

        dataframe = pd.read_csv(
            f'{path}/{MONTHLY_RATE_INPUT_FILENAME}.csv',
            sep=',',
            names=[
                'period_start',
                'period_end',
                'organization_id',
                'active_ther',
                'inactive_ther',
                'total_ther',
                'active_ther_b_period',
                'inactive_ther_b_period'
            ],
            dtype={
                'period_start': 'str',
                'period_end': 'str',
                'organization_id': 'Int64',
                'active_ther': 'Int64',
                'inactive_ther': 'Int64',
                'total_ther': 'Int64'
            },
            parse_dates=['period_start', 'period_end']
        )

        logger.info("Calculating monthly rate of the active therapists per Organization...")

        # Step 1 - Generate `churn_rate` column
        dataframe['churn_rate'] = dataframe.apply(
            lambda row: self._get_churn_rate(row['active_ther_b_period'], row['active_ther']),
            axis=1
        )

        # Step 2 - Generate `retention_rate` column
        dataframe['retention_rate'] = dataframe.apply(
            lambda row: self._get_retention_rate(row['active_ther_b_period'], row['active_ther']),
            axis=1
        )

        # Step 3 - Save results into CSV file
        output_path = f'{OUTPUT_RATE_PATH}/{ORG_RATE_DIR}/monthly'
        self._to_csv(dataframe, output_path, MONTHLY_RATE_OUTPUT_FILENAME)


class OrgYearlyActiveTherapistRate(ActiveTherapistRate):

    def __init__(self) -> None:

        # Runs data processor
        process_start_at = datetime.now()

        self._calculate()

        process_end_at = datetime.now()

        tag = "Calculating yearly rate of the active therapists per Organization"
        print_time_duration(tag, process_start_at, process_end_at)

    def _calculate(self) -> None:
        """
        Calculate rate of the active therapists per Organization
        and writes the result into CSV file.
        """
        logger.info("Load Organizations' yearly active therapists data from disk...")

        # Step 1 - Load Organizations' yearly active therapists
        path = f'{INPUT_RATE_PATH}/{ORG_RATE_DIR}/yearly'

        dataframe = pd.read_csv(
            f'{path}/{YEARLY_RATE_INPUT_FILENAME}.csv',
            sep=',',
            names=[
                'period_start',
                'period_end',
                'organization_id',
                'active_ther',
                'inactive_ther',
                'total_ther',
                'active_ther_b_period',
                'inactive_ther_b_period'
            ],
            dtype={
                'period_start': 'str',
                'period_end': 'str',
                'organization_id': 'Int64',
                'active_ther': 'Int64',
                'inactive_ther': 'Int64',
                'total_ther': 'Int64'
            },
            parse_dates=['period_start', 'period_end']
        )

        logger.info("Calculating monthly rate of the active therapists per Organization...")

        # Step 1 - Generate `churn_rate` column
        dataframe['churn_rate'] = dataframe.apply(
            lambda row: self._get_churn_rate(row['active_ther_b_period'], row['active_ther']),
            axis=1
        )

        # Step 2 - Generate `retention_rate` column
        dataframe['retention_rate'] = dataframe.apply(
            lambda row: self._get_retention_rate(row['active_ther_b_period'], row['active_ther']),
            axis=1
        )

        # Step 3 - Save results into CSV file
        output_path = f'{OUTPUT_RATE_PATH}/{ORG_RATE_DIR}/yearly'
        self._to_csv(dataframe, output_path, YEARLY_RATE_OUTPUT_FILENAME)


class TherapistRateProcessor:
    def __init__(self) -> None:

        # Runs data processor
        process_start_at = datetime.now()

        self._run_active_therapist_processor()

        OrgWeeklyActiveTherapistRate()
        OrgMonthlyActiveTherapistRate()
        OrgYearlyActiveTherapistRate()

        process_end_at = datetime.now()
        print_time_duration("Therapists' rates data processing", process_start_at, process_end_at)

    def _run_active_therapist_processor(self):
        """
        Run the active therapist data processor.
        """
        OrgWeeklyActiveTherProcessor()
        OrgMonthlyActiveTherProcessor()
        OrgYearlyActiveTherProcessor()


if __name__ == '__main__':
    configure_logging()

    TherapistRateProcessor()
