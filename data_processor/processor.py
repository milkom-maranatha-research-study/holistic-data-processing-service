import csv
import logging
import os

from dask import dataframe as dask_dataframe
from datetime import datetime
from typing import Tuple

from helpers import print_time_duration
from settings import configure_logging
from clients.operations import (
    InteractionBackendOperation,
    TherapistBackendOperation
)


logger = logging.getLogger(__name__)


NUM_OF_THERS_INPUT_PATH = 'input/num_of_ther'
NUM_OF_THERS_FILENAME = 'number-of-therapist'
NUM_OF_THERS_PER_ORG_FILENAME = 'number-of-therapist-per-org'

INTERACTION_INPUT_PATH = 'input/interaction'
INTERACTION_FILENAME = 'interaction'


class TherapistDataProcessor:

    def __init__(self) -> None:
        self.therapist_operation = TherapistBackendOperation()

        # Runs data processor
        process_start_at = datetime.now()

        self._process_data()

        process_end_at = datetime.now()
        print_time_duration("Therapist data processing", process_start_at, process_end_at)

    def _process_data(self) -> None:
        """
        Process Therapist data from Backend
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
        df_min_obj, df_max_obj = dask_dataframe.compute(
            cleaned_dataframe[['date_joined']].min(),
            cleaned_dataframe[['date_joined']].max()
        )
        min_date = df_min_obj['date_joined'].to_pydatetime()
        max_date = df_max_obj['date_joined'].to_pydatetime()
        num_all_therapist = cleaned_dataframe['id'].count().compute()

        logger.info("Count total therapists per Organization...")
        # * 3.2 Count the number of therapist per Organization.
        grouped_dataframe = cleaned_dataframe.groupby(['organization_id'])['id'].count()\
            .compute().reset_index(name='total_thers_in_org')

        # Step 4 - Save results into CSV files
        self._to_csv(
            ((min_date, max_date), num_all_therapist),
            grouped_dataframe
        )

    def _to_csv(
        self,
        tuple_all_time_ther: Tuple[Tuple[datetime], int],
        grouped_dataframe: dask_dataframe
    ) -> None:
        """
        Saves that `tuple_all_thers` and `grouped_dataframe` into CSV files.
        """
        is_exists = os.path.exists(NUM_OF_THERS_INPUT_PATH)

        if not is_exists:
            os.makedirs(NUM_OF_THERS_INPUT_PATH)

        # Write total therapists in NiceDay
        logger.info("Save total therapists in NiceDay into CSV file...")

        period, total_thers = tuple_all_time_ther
        period_start, period_end = period
        str_period_start = period_start.strftime('%Y-%m-%d')
        str_period_end = period_end.strftime('%Y-%m-%d')

        header = ['period', 'total_thers']
        data = [f'{str_period_start}/{str_period_end}', total_thers]

        with open(f'{NUM_OF_THERS_INPUT_PATH}/{NUM_OF_THERS_FILENAME}.csv', 'w') as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerow(data)

        # Write total therapists per Organization
        logger.info("Save total therapists per Organization into CSV files...")

        grouped_dataframe.to_csv(
            f'{NUM_OF_THERS_INPUT_PATH}/{NUM_OF_THERS_PER_ORG_FILENAME}.csv',
            index=False
        )


class InteractionDataProcessor:

    def __init__(self) -> None:
        self.therapist_operation = TherapistBackendOperation()
        self.ther_interaction_operation = InteractionBackendOperation()

        # Runs data processor
        process_start_at = datetime.now()

        self._process_data()

        process_end_at = datetime.now()
        print_time_duration("Interaction data processing", process_start_at, process_end_at)

    def _process_data(self) -> None:
        """
        Process Interaction data from Backend
        and writes it into multiple CSV files.
        """

        # Step 1 - Data Collection
        # * Get Interaction data from the Backend.
        self.ther_interaction_operation.collect_data()
        dataframe = self.ther_interaction_operation.data

        # Step 2 - Data Cleaning
        # * 2.1 Delete rows that doesn't have the Organization ID
        # *     For now, we consider those rows as dirty data,
        # *     We assume every therapist must be a member of the Organization.
        logger.info("Removing rows with no Organization ID...")
        dataframe = dataframe.dropna(subset=['organization_id'])

        # * 2.2 Delete rows that has `chat_count` <= 1 and `call_count` < 1.
        # *     We assume those rows are invalid, consider that:
        # *     a. Interaction is valid when the therapist send a chat
        # *        to their clients more than once.
        # *        It means they replied to the client's chat message. OR
        # *     b. Interaction is valid when the therapist have a call
        # *        with the client at least once.
        # *        It means the therapist and the client talked to each other.
        logger.info("Removing invalid interactions...")
        dataframe = dataframe[
            (dataframe['chat_count'] > 1) | (dataframe['call_count'] >= 1)
        ]

        # Step 3 - Data Distinction
        # * We need to distinct the data rows based on the therapist ID
        # * and interaction date.
        #
        # * We assume that if the therapist interacts with multiple clients
        # * in the same day, we only need to pick one.
        #
        # * It's sufficient (for now) to tells that therapist is active on that day.
        logger.info("Distinct data by interaction date...")
        cleaned_dataframe = dataframe.drop_duplicates(
            subset=['therapist_id', 'interaction_date'],
            keep='last'
        )

        # Step 4 - Merge Interaction data with the therapist data
        # * 4.1 Merge all-time interaction dataframe with the all-time therapist dataframe
        # *     to generate new columns called `all_time_period` and `all_time_thers`.
        # *
        # *     This is required by the aggregator service, so it can calculate
        # *     the all-time number of active/inactive therapists in NiceDay.
        logger.info("Merge all-time interaction dataframe with the all-time therapist dataframe...")
        all_time_therapist = dask_dataframe.read_csv(
            f'{NUM_OF_THERS_INPUT_PATH}/{NUM_OF_THERS_FILENAME}.csv',
            dtype={
                'period': 'str',
                'total_thers': 'Int64'
            }
        )
        head = all_time_therapist.head()
        all_time_dataframe = cleaned_dataframe.assign(
            all_time_period=lambda _: head.iloc[0][0],
            all_time_thers=lambda _: head.iloc[0][1],
        )

        # * 4.2 Merge interaction dataframe with the therapist dataframe
        # *     to generate a new column called `total_therapists_in_org`.
        # *
        # *     This is required by the aggregator service, so it can calculate
        # *     the number of active/inactive therapists per org in one go.
        logger.info("Merge interaction dataframe with the therapist dataframe...")
        num_thers_per_org_dataframe = dask_dataframe.read_csv(
            f'{NUM_OF_THERS_INPUT_PATH}/{NUM_OF_THERS_PER_ORG_FILENAME}.csv',
            dtype={
                'organization_id': 'Int64',
                'total_thers_in_org': 'Int64'
            }
        )

        merged_dataframe = cleaned_dataframe.merge(
            num_thers_per_org_dataframe,
            how='left',
            on='organization_id'
        )

        # Step 5 - Generate period column
        # * Value of the `period` column is generated based on the `interaction_date`.
        logger.info("Generate 'period' column based on the period type and 'interaction_date'...")
        weekly_dataframe = merged_dataframe.assign(
            period=lambda x: x.interaction_date.dt.to_period('W')
        )
        monthly_dataframe = merged_dataframe.assign(
            period=lambda x: x.interaction_date.dt.to_period('M')
        )
        yearly_dataframe = merged_dataframe.assign(
            period=lambda x: x.interaction_date.dt.to_period('Y')
        )

        # * Step 6 - Distinct interaction data by `therapist_id` and `period` columns.
        # * We need this disctinction is required to remove duplicate therapists
        # * that are active within that period.
        logger.info("Distinct data by the 'therapist_id' and 'period' columns...")
        all_time_dataframe = all_time_dataframe.drop_duplicates(
            subset=['therapist_id', 'all_time_period'],
            keep='last'
        )
        weekly_dataframe = weekly_dataframe.drop_duplicates(
            subset=['therapist_id', 'period'],
            keep='last'
        )
        monthly_dataframe = monthly_dataframe.drop_duplicates(
            subset=['therapist_id', 'period'],
            keep='last'
        )
        yearly_dataframe = yearly_dataframe.drop_duplicates(
            subset=['therapist_id', 'period'],
            keep='last'
        )

        # * 7 - Save the final weekly dataframe into CSV 
        logger.info("Save Interaction data for all-time period into CSV files...")
        self._to_csv(all_time_dataframe, 'alltime')

        logger.info("Save Interaction data for weekly periods into CSV files...")
        self._to_csv(weekly_dataframe, 'weekly')

        logger.info("Save Interaction data for monthly periods into CSV files...")
        self._to_csv(monthly_dataframe, 'monthly')

        logger.info("Save Interaction data for yearly periods into CSV files...")
        self._to_csv(yearly_dataframe, 'yearly')

    def _to_csv(
        self,
        dataframe: dask_dataframe,
        period_type: str
    ) -> None:
        """
        Slices and saves that `dataframe` into multiple CSV files.
        """
        # Check the input directory availability
        is_exists = os.path.exists(INTERACTION_INPUT_PATH)

        if not is_exists:
            os.makedirs(INTERACTION_INPUT_PATH)

        # Check the period directory availability
        path = f'{INTERACTION_INPUT_PATH}/{period_type}'

        is_exists = os.path.exists(path)

        if not is_exists:
            os.makedirs(path)

        # Slice dataframe into 10 partitions
        dataframe = dataframe.repartition(npartitions=10)

        # Save and simplifies interaction dataframe
        if period_type == 'alltime':
            dataframe[['all_time_period', 'all_time_thers', 'therapist_id']].to_csv(
                f'{path}/{INTERACTION_FILENAME}-part-*.csv',
                index=False,
                header=False
            )
        else:
            dataframe[['period', 'organization_id', 'total_thers_in_org', 'therapist_id']].to_csv(
                f'{path}/{INTERACTION_FILENAME}-part-*.csv',
                index=False,
                header=False
            )


if __name__ == '__main__':
    configure_logging()

    TherapistDataProcessor()
    InteractionDataProcessor()
