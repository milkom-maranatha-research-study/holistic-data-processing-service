import logging
import os

from dask import dataframe as dask_dataframe
from datetime import datetime

from data_processor.src.clients.operations import (
    InteractionBackendOperation,
    TherapistBackendOperation
)
from data_processor.src.helpers import print_time_duration
from data_processor.settings import configure_logging


logger = logging.getLogger(__name__)


AGG_NUM_OF_THERS_PATH = 'output/num-of-ther'
AGG_NUM_OF_THERS_FILENAME = 'all-aggregate'
AGG_NUM_OF_THERS_PER_ORG_FILENAME = 'per-org-aggregate'

INPUT_INTERACTION_PATH = 'input/interaction'
INPUT_APP_INTERACTION_PATH = 'input/interaction/by-app'
INPUT_ORG_INTERACTION_PATH = 'input/interaction/by-org'

ALL_INTERACTION_FILENAME = 'all-interaction'
APP_INTERACTION_FILENAME = 'app-interaction'
ORG_INTERACTION_FILENAME = 'org-interaction'


class InteractionDataProcessor:

    def __init__(self) -> None:
        self.therapist_operation = TherapistBackendOperation()
        self.ther_interaction_operation = InteractionBackendOperation()
        self.exporter = InteractionFileExport()

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
        logger.info("Distinct data by therapist and its interaction date...")
        cleaned_dataframe = dataframe.drop_duplicates(
            subset=['therapist_id', 'interaction_date'],
            keep='last'
        )

        # Step 4 - Merge Interaction data with the therapist data
        # * Merge the cleaned interaction dataframe with the all-time therapist dataframe
        # * to generate new columns called `all_time_period` and `all_time_thers`.
        # *
        # * This is required by the aggregator service, so it can calculate
        # * the number of active/inactive therapists in one go.
        logger.info("Merge the interaction dataframe with the all-time therapist dataframe...")
        num_of_thers_dataframe = dask_dataframe.read_csv(
            f'{AGG_NUM_OF_THERS_PATH}/{AGG_NUM_OF_THERS_FILENAME}.csv',
            sep='\t',
            names=['all_time_period', 'all_time_thers'],
            dtype={
                'all_time_period': 'str',
                'total_thers': 'Int64'
            }
        )

        head = num_of_thers_dataframe.head()
        merged_dataframe = cleaned_dataframe.assign(
            all_time_period=lambda _: head.iloc[0][0],
            all_time_thers=lambda _: head.iloc[0][1],
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

        # Step 6 - Distinct interaction data by `therapist_id` and `period` columns.
        # * We need this disctinction is required to remove duplicate therapists
        # * that are active within that period.
        logger.info("Distinct data by the 'therapist_id' and 'period' columns...")
        all_time_dataframe = merged_dataframe.drop_duplicates(
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

        # 7 - Create input files with CSV format
        self.exporter.to_csv(all_time_dataframe, 'alltime')

        self.exporter.to_csv(weekly_dataframe, period_type='weekly')
        self.exporter.to_csv(monthly_dataframe, period_type='monthly')
        self.exporter.to_csv(yearly_dataframe, period_type='yearly')

        self.exporter.to_csv(weekly_dataframe, period_type='weekly', per_org=True)
        self.exporter.to_csv(monthly_dataframe, period_type='monthly', per_org=True)
        self.exporter.to_csv(yearly_dataframe, period_type='yearly', per_org=True)


class InteractionFileExport:

    def to_csv(
        self,
        dataframe: dask_dataframe,
        period_type: str,
        per_org: bool = False
    ) -> None:
        """
        Exports that `dataframe` into CSV files.
        """

        if period_type == 'alltime':
            self._all(dataframe)
            return

        if per_org:
            self._per_org(dataframe, period_type)
        else:
            self._per_app(dataframe, period_type)

    def _all(self, dataframe: dask_dataframe) -> None:
        """
        Create input files from that `dataframe` with CSV format
        for all-time period.
        """
        logger.info("Save Interaction data for all-time period into CSV files...")

        # Check the period input directory availability
        path = f'{INPUT_INTERACTION_PATH}/alltime'

        is_exists = os.path.exists(path)

        if not is_exists:
            os.makedirs(path)

        # Slice dataframe into 10 partitions
        dataframe = dataframe.repartition(npartitions=10)

        # Save and simplifies interaction dataframe
        dataframe[['all_time_period', 'all_time_thers', 'therapist_id']].to_csv(
            f'{path}/{ALL_INTERACTION_FILENAME}-part-*.csv',
            index=False,
            header=False
        )

    def _per_org(
        self,
        dataframe: dask_dataframe,
        period_type: str
    ) -> None:
        """
        Create input files per Organization from that `dataframe` with CSV format
        for that speficic `period_type`.
        """
        # Additional Step - Merge interaction dataframe with the therapist per org dataframe
        # * This is required by the aggregator service, so it can calculate
        # * the number of active/inactive therapists per org in one go.

        logger.info("Merge interaction dataframe with the therapist per org dataframe...")

        num_thers_per_org_dataframe = dask_dataframe.read_csv(
            f'{AGG_NUM_OF_THERS_PATH}/{AGG_NUM_OF_THERS_PER_ORG_FILENAME}.csv',
            sep='\t',
            names=['period', 'organization_id', 'total_thers_in_org'],
            dtype={
                'period': 'str',
                'organization_id': 'Int64',
                'total_thers_in_org': 'Int64'
            }
        )[['organization_id', 'total_thers_in_org']]

        dataframe = dataframe.merge(
            num_thers_per_org_dataframe,
            how='left',
            on='organization_id'
        )

        logger.info(f"Save Interaction data per Organization for {period_type} period into CSV files...")

        # Create the period input directory if it doesn't exists
        path = f'{INPUT_ORG_INTERACTION_PATH}/{period_type}'

        is_exists = os.path.exists(path)

        if not is_exists:
            os.makedirs(path)

        # Slice dataframe into 10 partitions
        dataframe = dataframe.repartition(npartitions=10)

        # Save and simplifies interaction dataframe
        dataframe[['period', 'organization_id', 'total_thers_in_org', 'therapist_id']].to_csv(
            f'{path}/{ORG_INTERACTION_FILENAME}-part-*.csv',
            index=False,
            header=False
        )

    def _per_app(
        self,
        dataframe: dask_dataframe,
        period_type: str
    ) -> None:
        """
        Create input files per NiceDay Application from that `dataframe` with CSV format
        for that speficic `period_type`.
        """
        logger.info(f"Save Interaction data per App for {period_type} period into CSV files...")

        # Create the period input directory if it doesn't exists
        path = f'{INPUT_APP_INTERACTION_PATH}/{period_type}'

        is_exists = os.path.exists(path)

        if not is_exists:
            os.makedirs(path)

        # Slice dataframe into 10 partitions
        dataframe = dataframe.repartition(npartitions=10)

        # Save and simplifies interaction dataframe
        dataframe[['period', 'all_time_thers', 'therapist_id']].to_csv(
            f'{path}/{APP_INTERACTION_FILENAME}-part-*.csv',
            index=False,
            header=False
        )


if __name__ == '__main__':
    configure_logging()

    InteractionDataProcessor()
