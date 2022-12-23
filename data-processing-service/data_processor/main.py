import csv
import logging
import os

from dask import dataframe as dask_dataframe
from datetime import datetime
from typing import List, Tuple

from helpers import print_time_duration
from settings import configure_logging
from sources.dateutils import DateUtil
from sources.operations import (
    InteractionBackendOperation,
    TherapistBackendOperation
)


logger = logging.getLogger(__name__)


class TherapistDataProcessor:
    INPUT_PATH = 'input/num_of_thers'

    def __init__(self) -> None:
        self.therapist_operation = TherapistBackendOperation()

        process_start_at = datetime.now()

        self._process_data()

        process_end_at = datetime.now()
        print_time_duration("Therapist data processing", process_start_at, process_end_at)

    def _process_data(self) -> None:
        """
        Process therapists data from Backend
        and writes the total therapists into multiple CSV files.
        """

        # Step 1 - Collect Data
        # * Get therapist data from the Backend.
        self.therapist_operation.collect_data()
        dataframe = self.therapist_operation.data

        logger.info("Cleaning data...")
        # Step 2 - Clean Data
        # * Delete rows that doesn't have the Organization ID
        # * For now, we consider those rows as dirty data,
        # * We assume every therapist must be a member of the Organization.
        cleaned_dataframe = dataframe.dropna(subset=['organization_id'])

        logger.info("Count all therapist in NiceDay...")
        # Step 3 - Count Data
        # * 3.1 Count all of therapist in NiceDay.
        num_all_therapist = cleaned_dataframe['id'].count().compute()

        logger.info("Count total therapists per Organization...")
        # * 3.2 Count the number of therapist per Organization.
        grouped_dataframe = cleaned_dataframe.groupby(['organization_id'])['id'].count()\
            .compute().reset_index(name='total_thers')

        # Step 4 - Save results into CSV files
        self._to_csv(num_all_therapist, grouped_dataframe)

    def _to_csv(
        self,
        num_all_therapist: int,
        grouped_dataframe: dask_dataframe
    ) -> None:
        """
        Saves that `num_all_therapist` and `grouped_dataframe` into CSV files.
        """
        is_exists = os.path.exists(self.INPUT_PATH)

        if not is_exists:
            os.makedirs(self.INPUT_PATH)

        # Write total therapists in NiceDay
        logger.info("Save total therapists in NiceDay into CSV file...")

        header = ['total_thers']
        data = [num_all_therapist]

        with open(f'{self.INPUT_PATH}/number-of-therapist.csv', 'w') as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerow(data)

        # Write total therapists per Organization
        logger.info("Save total therapists per Organization into CSV files...")

        grouped_dataframe.to_csv(
            f'{self.INPUT_PATH}/number-of-therapist-per-org.csv',
            index=False
        )


class InteractionDataProcessor:
    INPUT_PATH = 'input/interaction'

    def __init__(self) -> None:
        self.ther_interaction_operation = InteractionBackendOperation()
        self.date_util = DateUtil()

        process_start_at = datetime.now()

        self._process_data()

        process_end_at = datetime.now()
        print_time_duration("Interaction data processing", process_start_at, process_end_at)

    def _process_data(self) -> None:
        """
        Process therapists' interactions from Backend
        and writes it into multiple CSV files.
        """

        # Step 1 - Collect Data
        # * Get therapists' interactions from the Backend.
        self.ther_interaction_operation.collect_data()
        dataframe = self.ther_interaction_operation.data

        logger.info("Cleaning data...")
        # Step 2 - Clean Data
        # * 2.1 Delete rows that doesn't have the Organization ID
        # *     For now, we consider those rows as dirty data,
        # *     We assume every therapist must be a member of the Organization.
        dataframe = dataframe.dropna(subset=['organization_id'])

        # * 2.2 Delete rows that has `chat_count` <= 1 and `call_count` < 1.
        # *     We assume those rows are invalid, consider that:
        # *     a. Therapist's interaction is valid when they send a chat
        # *        to their clients more than once.
        # *        It means they replied to the client's chat message. OR
        # *     b. Therapist's interaction is valid when they have a call
        # *        with the client at least once.
        # *        It means the therapist and the client talked to each other.
        dataframe = dataframe[
            (dataframe['chat_count'] > 1) | (dataframe['call_count'] >= 1)
        ]

        logger.info("Distinct data by interaction date...")
        # Step 3 - Data Distinction
        # * We need to distinct the data rows based on the therapist ID
        # * and interaction date.
        # 
        # * We assume that if the therapist interacts with multiple clients
        # * in the same day, we only need to pick one.
        # 
        # * It's sufficient (for now) to tells that therapist is active on that day.
        cleaned_dataframe = dataframe.drop_duplicates(
            subset=['therapist_id', 'interaction_date'],
            keep='last'
        )

        # Step 4 - Save results into CSV files
        is_exists = os.path.exists(self.INPUT_PATH)

        if not is_exists:
            os.makedirs(self.INPUT_PATH)

        logger.info("Generating available time periods from the data...")
        start_date, end_date = self._get_interaction_period(cleaned_dataframe)

        logger.info("Save Interaction data in weekly period into CSV files...")
        self._to_csv(start_date, end_date, 'weekly', cleaned_dataframe)

        logger.info("Save Interaction data in monthly period into CSV files...")
        self._to_csv(start_date, end_date, 'monthly', cleaned_dataframe)

        logger.info("Save Interaction data in yearly period into CSV files...")
        self._to_csv(start_date, end_date, 'yearly', cleaned_dataframe)

    def _to_csv(
        self,
        start_date: datetime,
        end_date: datetime,
        period_type: str,
        dataframe: dask_dataframe
    ) -> None:
        """
        Slices and saves that `dataframe` into multiple CSV files in a specific `period_type`,
        starting from that given `start_date` until the given `end_date`.
        """

        if period_type not in ['weekly', 'monthly', 'yearly']:
            raise ValueError("'period_type' is invalid.")

        # Create directory if not exists
        path = f'{self.INPUT_PATH}/{period_type}'

        is_exists = os.path.exists(path)

        if not is_exists:
            os.makedirs(path)

        # Slicing dataframe in a specific period.
        periods = self._get_periods_from(start_date, end_date, period_type)

        for start, end in periods:

            start_str, end_str = start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')
            logger.info(f"Slice {period_type} Interaction data from {start_str} to {end_str}...")

            # Filters dataframe
            sliced_dataframe = dataframe[
                (dataframe['interaction_date'] >= start) & (dataframe['interaction_date'] <= end)
            ]

            # Assign time period into dataframe
            sliced_dataframe = sliced_dataframe.assign(
                period=lambda _: f"{start_str}_{end_str}"
            ).compute()

            # Save to CSV file
            logger.info(f"Create {period_type} CSV file from {start_str} to {end_str}...")
            sliced_dataframe.to_csv(
                f'{path}/interactions-' + f'{start_str}_{end_str}.csv',
                index=False
            )

    def _get_interaction_period(self, dataframe: dask_dataframe) -> List[Tuple]:
        """
        Returns Tuple of the start and end date of the therapist interaction
        in that given `dataframe`.
        """

        df_min_obj, df_max_obj = dask_dataframe.compute(
            dataframe[['interaction_date']].min(),
            dataframe[['interaction_date']].max()
        )

        start_date = df_min_obj['interaction_date'].to_pydatetime()
        end_date = df_max_obj['interaction_date'].to_pydatetime()

        return start_date, end_date

    def _get_periods_from(
        self, 
        start_date: datetime,
        end_date: datetime,
        period_type: str
    ) -> List[Tuple]:
        """
        Returns list of date period from the given `start_date` until the `end_date`
        with a specific `period_type`.
        """

        if period_type == 'weekly':
            return self.date_util.get_weekly_periods(start_date, end_date)

        elif period_type == 'monthly':
            return self.date_util.get_monthly_periods(start_date, end_date)

        elif period_type == 'yearly':
            return self.date_util.get_yearly_periods(start_date, end_date)

        return []


if __name__ == '__main__':
    configure_logging()

    TherapistDataProcessor()
    InteractionDataProcessor()
