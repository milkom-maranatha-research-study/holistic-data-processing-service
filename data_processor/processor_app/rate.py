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


ORG_WEEKLY_RATE_INPUT_PATH = 'input/org-rate/weekly'
ORG_WEEKLY_RATE_INPUT_FILENAME = 'input-rate-weekly'

ORG_MONTHLY_RATE_INPUT_PATH = 'input/org-rate/monthly'
ORG_MONTHLY_RATE_INPUT_FILENAME = 'input-rate-monthly'

ORG_YEARLY_RATE_INPUT_PATH = 'input/org-rate/yearly'
ORG_YEARLY_RATE_INPUT_FILENAME = 'input-rate-yearly'

ORG_RATE_OUTPUT_PATH = 'output/org/rate'
ORG_WEEKLY_RATE_OUTPUT_FILENAME = 'org-weekly-rate'
ORG_MONTHLY_RATE_OUTPUT_FILENAME = 'org-monthly-rate'
ORG_YEARLY_RATE_OUTPUT_FILENAME = 'org-yearly-rate'


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
        dataframe = pd.read_csv(
            f'{ORG_WEEKLY_RATE_INPUT_PATH}/{ORG_WEEKLY_RATE_INPUT_FILENAME}.csv',
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
        self._to_csv(dataframe, ORG_RATE_OUTPUT_PATH, ORG_WEEKLY_RATE_OUTPUT_FILENAME)


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
        dataframe = pd.read_csv(
            f'{ORG_MONTHLY_RATE_INPUT_PATH}/{ORG_MONTHLY_RATE_INPUT_FILENAME}.csv',
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
        self._to_csv(dataframe, ORG_RATE_OUTPUT_PATH, ORG_MONTHLY_RATE_OUTPUT_FILENAME)


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
        dataframe = pd.read_csv(
            f'{ORG_YEARLY_RATE_INPUT_PATH}/{ORG_YEARLY_RATE_INPUT_FILENAME}.csv',
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
        self._to_csv(dataframe, ORG_RATE_OUTPUT_PATH, ORG_YEARLY_RATE_OUTPUT_FILENAME)


class OrganizationRate:

    def __init__(self) -> None:
        OrgWeeklyActiveTherProcessor()
        OrgMonthlyActiveTherProcessor()
        OrgYearlyActiveTherProcessor()

        OrgWeeklyActiveTherapistRate()
        OrgMonthlyActiveTherapistRate()
        OrgYearlyActiveTherapistRate()


if __name__ == '__main__':
    configure_logging()

    OrganizationRate()
