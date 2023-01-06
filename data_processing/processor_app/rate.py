import logging
import os
import pandas as pd

from datetime import datetime

from data_processing.src.helpers import print_time_duration
from data_processing.settings import configure_logging


logger = logging.getLogger(__name__)


ORG_RATE_DIR = 'by-org'
APP_RATE_DIR = 'by-app'

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
        logger.info("Load input of weekly active therapists per Organization from disk...")

        # Step 1 - Load input of weekly active therapists
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
                'total_ther': 'Int64',
                'active_ther_b_period': 'Int64',
                'inactive_ther_b_period': 'Int64'
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

        logger.info("Save therapists' rates data into CSV file...")

        # Step 3 - Save results into CSV file
        # * Create output directory if it doesn't exists
        output_path = f'{OUTPUT_RATE_PATH}/{ORG_RATE_DIR}/weekly'

        is_exists = os.path.exists(output_path)

        if not is_exists:
            os.makedirs(output_path)

        # * Export dataframe to CSV file
        dataframe[[
            'period_start',
            'period_end',
            'organization_id',
            'churn_rate',
            'retention_rate',
        ]].to_csv(
            f'{output_path}/{WEEKLY_RATE_OUTPUT_FILENAME}.csv',
            index=False,
            header=False
        )


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
        logger.info("Load input of monthly active therapists per Organization from disk...")

        # Step 1 - Load input of monthly active therapists
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
                'total_ther': 'Int64',
                'active_ther_b_period': 'Int64',
                'inactive_ther_b_period': 'Int64'
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

        logger.info("Save therapists' rates data into CSV file...")

        # Step 3 - Save results into CSV file
        # * Create output directory if it doesn't exists
        output_path = f'{OUTPUT_RATE_PATH}/{ORG_RATE_DIR}/monthly'

        is_exists = os.path.exists(output_path)

        if not is_exists:
            os.makedirs(output_path)

        # * Export dataframe to CSV file
        dataframe[[
            'period_start',
            'period_end',
            'organization_id',
            'churn_rate',
            'retention_rate',
        ]].to_csv(
            f'{output_path}/{MONTHLY_RATE_OUTPUT_FILENAME}.csv',
            index=False,
            header=False
        )


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
        logger.info("Load input of yearly active therapists per Organization from disk...")

        # Step 1 - Load input of yearly active therapists
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
                'total_ther': 'Int64',
                'active_ther_b_period': 'Int64',
                'inactive_ther_b_period': 'Int64'
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

        logger.info("Save therapists' rates data into CSV file...")

        # Step 3 - Save results into CSV file
        # * Create output directory if it doesn't exists
        output_path = f'{OUTPUT_RATE_PATH}/{ORG_RATE_DIR}/yearly'

        is_exists = os.path.exists(output_path)

        if not is_exists:
            os.makedirs(output_path)

        # * Export dataframe to CSV file
        dataframe[[
            'period_start',
            'period_end',
            'organization_id',
            'churn_rate',
            'retention_rate',
        ]].to_csv(
            f'{output_path}/{YEARLY_RATE_OUTPUT_FILENAME}.csv',
            index=False,
            header=False
        )


class NDWeeklyActiveTherapistRate(ActiveTherapistRate):

    def __init__(self) -> None:

        # Runs data processor
        process_start_at = datetime.now()

        self._calculate()

        process_end_at = datetime.now()

        tag = "Calculating weekly rate of the active therapists in NiceDay"
        print_time_duration(tag, process_start_at, process_end_at)

    def _calculate(self) -> None:
        """
        Calculate rate of the active therapists in NiceDay
        and writes the result into CSV file.
        """
        logger.info("Load input of weekly active therapists in NiceDay from disk...")

        # Step 1 - Load input of weekly active therapists
        path = f'{INPUT_RATE_PATH}/{APP_RATE_DIR}/weekly'

        dataframe = pd.read_csv(
            f'{path}/{WEEKLY_RATE_INPUT_FILENAME}.csv',
            sep=',',
            names=[
                'period_start',
                'period_end',
                'active_ther',
                'inactive_ther',
                'total_ther',
                'active_ther_b_period',
                'inactive_ther_b_period'
            ],
            dtype={
                'period_start': 'str',
                'period_end': 'str',
                'active_ther': 'Int64',
                'inactive_ther': 'Int64',
                'total_ther': 'Int64',
                'active_ther_b_period': 'Int64',
                'inactive_ther_b_period': 'Int64'
            },
            parse_dates=['period_start', 'period_end']
        )

        logger.info("Calculating weekly rate of the active therapists in NiceDay...")

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

        logger.info("Save therapists' rates data into CSV file...")

        # Step 3 - Save results into CSV file
        # * Create output directory if it doesn't exists
        output_path = f'{OUTPUT_RATE_PATH}/{APP_RATE_DIR}/weekly'

        is_exists = os.path.exists(output_path)

        if not is_exists:
            os.makedirs(output_path)

        # * Export dataframe to CSV file
        dataframe[[
            'period_start',
            'period_end',
            'churn_rate',
            'retention_rate',
        ]].to_csv(
            f'{output_path}/{WEEKLY_RATE_OUTPUT_FILENAME}.csv',
            index=False,
            header=False
        )


class NDMonthlyActiveTherapistRate(ActiveTherapistRate):

    def __init__(self) -> None:

        # Runs data processor
        process_start_at = datetime.now()

        self._calculate()

        process_end_at = datetime.now()

        tag = "Calculating monthly rate of the active therapists in NiceDay"
        print_time_duration(tag, process_start_at, process_end_at)

    def _calculate(self) -> None:
        """
        Calculate rate of the active therapists in NiceDay
        and writes the result into CSV file.
        """
        logger.info("Load input of monthly active therapists in NiceDay from disk...")

        # Step 1 - Load input of monthly active therapists
        path = f'{INPUT_RATE_PATH}/{APP_RATE_DIR}/monthly'

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
                'total_ther': 'Int64',
                'active_ther_b_period': 'Int64',
                'inactive_ther_b_period': 'Int64'
            },
            parse_dates=['period_start', 'period_end']
        )

        logger.info("Calculating monthly rate of the active therapists in NiceDay...")

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

        logger.info("Save therapists' rates data into CSV file...")

        # Step 3 - Save results into CSV file
        # * Create output directory if it doesn't exists
        output_path = f'{OUTPUT_RATE_PATH}/{APP_RATE_DIR}/monthly'

        is_exists = os.path.exists(output_path)

        if not is_exists:
            os.makedirs(output_path)

        # * Export dataframe to CSV file
        dataframe[[
            'period_start',
            'period_end',
            'churn_rate',
            'retention_rate',
        ]].to_csv(
            f'{output_path}/{MONTHLY_RATE_OUTPUT_FILENAME}.csv',
            index=False,
            header=False
        )


class NDYearlyActiveTherapistRate(ActiveTherapistRate):

    def __init__(self) -> None:

        # Runs data processor
        process_start_at = datetime.now()

        self._calculate()

        process_end_at = datetime.now()

        tag = "Calculating yearly rate of the active therapists in NiceDay"
        print_time_duration(tag, process_start_at, process_end_at)

    def _calculate(self) -> None:
        """
        Calculate rate of the active therapists in NiceDay
        and writes the result into CSV file.
        """
        logger.info("Load input of yearly active therapists in NiceDay from disk...")

        # Step 1 - Load input of yearly active therapists
        path = f'{INPUT_RATE_PATH}/{APP_RATE_DIR}/yearly'

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
                'total_ther': 'Int64',
                'active_ther_b_period': 'Int64',
                'inactive_ther_b_period': 'Int64'
            },
            parse_dates=['period_start', 'period_end']
        )

        logger.info("Calculating yearly rate of the active therapists in NiceDay...")

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

        logger.info("Save therapists' rates data into CSV file...")

        # Step 3 - Save results into CSV file
        # * Create output directory if it doesn't exists
        output_path = f'{OUTPUT_RATE_PATH}/{APP_RATE_DIR}/yearly'

        is_exists = os.path.exists(output_path)

        if not is_exists:
            os.makedirs(output_path)

        # * Export dataframe to CSV file
        dataframe[[
            'period_start',
            'period_end',
            'churn_rate',
            'retention_rate',
        ]].to_csv(
            f'{output_path}/{YEARLY_RATE_OUTPUT_FILENAME}.csv',
            index=False,
            header=False
        )


class TherapistRateProcessor:
    def __init__(self) -> None:

        # Runs data processor
        process_start_at = datetime.now()

        OrgWeeklyActiveTherapistRate()
        OrgMonthlyActiveTherapistRate()
        OrgYearlyActiveTherapistRate()

        NDWeeklyActiveTherapistRate()
        NDMonthlyActiveTherapistRate()
        NDYearlyActiveTherapistRate()

        process_end_at = datetime.now()
        print_time_duration("Therapists' rates data processing", process_start_at, process_end_at)


if __name__ == '__main__':
    configure_logging()

    TherapistRateProcessor()
