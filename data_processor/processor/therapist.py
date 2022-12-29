import logging
import os

from dask import dataframe as dask_dataframe
from datetime import datetime

from data_processor.clients.operations import TherapistBackendOperation
from data_processor.helpers import print_time_duration
from data_processor.settings import configure_logging


logger = logging.getLogger(__name__)


THERAPIST_INPUT_PATH = 'input/therapist'
THERAPIST_FILENAME = 'therapist'


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

        # Step 3 - Generate all-time period column
        df_min_obj, df_max_obj = dask_dataframe.compute(
            cleaned_dataframe[['date_joined']].min(),
            cleaned_dataframe[['date_joined']].max()
        )
        min_date = df_min_obj['date_joined'].to_pydatetime().strftime('%Y-%m-%d')
        max_date = df_max_obj['date_joined'].to_pydatetime().strftime('%Y-%m-%d')
        cleaned_dataframe = cleaned_dataframe.assign(
            all_time_period=lambda _: f"{min_date}/{max_date}"
        )

        # Step 4 - Save results into CSV files
        self._to_csv(cleaned_dataframe)

    def _to_csv(self, dataframe: dask_dataframe) -> None:
        """
        Saves that `dataframe` into CSV files.
        """
        is_exists = os.path.exists(THERAPIST_INPUT_PATH)

        if not is_exists:
            os.makedirs(THERAPIST_INPUT_PATH)

        # Slice dataframe into 10 partitions
        dataframe = dataframe.repartition(npartitions=10)

        # Save and simplifies therapist dataframe
        logger.info("Save Therapist data into CSV files...")

        dataframe[['all_time_period', 'organization_id', 'id']].to_csv(
            f'{THERAPIST_INPUT_PATH}/{THERAPIST_FILENAME}-part-*.csv',
            index=False,
            header=False
        )


if __name__ == '__main__':
    configure_logging()

    TherapistDataProcessor()
