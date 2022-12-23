import csv
import logging
import os

from dask import dataframe as dask_dataframe

from settings import configure_logging
from sources.operations import (
    InteractionBackendOperation,
    TherapistBackendOperation
)


logger = logging.getLogger(__name__)


class TherapistProcessor:
    INPUT_PATH = 'input/num_of_thers'

    def __init__(self) -> None:
        self.therapist_operation = TherapistBackendOperation()

        self._process_data()

    def _process_data(self) -> None:
        """
        Process therapists data from Backend
        and writes the total therapists into multiple CSV files.
        """

        # Step 1 - Collect Data
        # * Get therapist data from the Backend.
        self.therapist_operation.collect_data()
        dataframe = self.therapist_operation.data

        # Step 2 - Clean Data
        # * Delete rows that doesn't have the Organization ID
        # * For now, we consider those rows as dirty data,
        # * We assume every therapist must be a member of the Organization.
        cleaned_dataframe = dataframe.dropna(subset=['organization_id'])

        # Step 3 - Count Data
        # * 3.1 Count all of therapist in NiceDay.
        num_all_therapist = cleaned_dataframe['id'].count().compute()

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
        header = ['total_thers']
        data = [num_all_therapist]

        with open(f'{self.INPUT_PATH}/number-of-therapist.csv', 'w') as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerow(data)

        # Write total therapists per Organization
        grouped_dataframe.to_csv(
            f'{self.INPUT_PATH}/number-of-therapist-per-org.csv',
            index=False
        )


class InteractionProcessor:
    INPUT_PATH = 'input/interaction'

    def __init__(self) -> None:
        self.ther_interaction_operation = InteractionBackendOperation()

        self._process_data()

    def _process_data(self) -> None:
        """
        Process therapists' interactions from Backend
        and writes it into multiple CSV files.
        """

        # Step 1 - Collect Data
        # * Get therapists' interactions from the Backend.
        self.ther_interaction_operation.collect_data()
        dataframe = self.ther_interaction_operation.data

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
        self._to_csv(cleaned_dataframe)

    def _to_csv(self, interaction_dataframe: dask_dataframe) -> None:
        """
        Saves that `interaction_dataframe` into CSV files.
        """
        is_exists = os.path.exists(self.INPUT_PATH)

        if not is_exists:
            os.makedirs(self.INPUT_PATH)

        interaction_dataframe.compute().to_csv(
            f'{self.INPUT_PATH}/thers-interactions.csv',
            index=False
        )


if __name__ == '__main__':
    configure_logging()

    TherapistProcessor()
    InteractionProcessor()
