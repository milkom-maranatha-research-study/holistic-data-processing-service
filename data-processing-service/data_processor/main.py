import logging
import os

from settings import configure_logging
from sources.operations import (
    InteractionBackendOperation,
    TherapistBackendOperation
)


logger = logging.getLogger(__name__)


class TherapistProcessor:
    INPUT_PATH = 'tmp/input'

    def __init__(self) -> None:
        self.therapist_operation = TherapistBackendOperation()

        self._process_data()

    def _process_data(self) -> None:
        """
        Process therapists data from Backend and writes the total therapists
        in NiceDay to a file.
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
        # * We need to count the overall number of therapist in NiceDay
        # * as an input for the data aggregator.
        size = cleaned_dataframe['id'].compute().size

        # Step 4 - Writes result
        # * 4.1 Write total therapists in NiceDay
        is_exists = os.path.exists(self.INPUT_PATH)

        if not is_exists:
            os.makedirs(self.INPUT_PATH)

        with open(f'{self.INPUT_PATH}/number-of-therapist.csv', 'w') as file:
            file.write(f'{size}')

        # * 4.2 Write total therapists per Organization
        grouped_dataframe = cleaned_dataframe.groupby(['organization_id'])['id'].count()\
            .compute().reset_index(name='total_thers')

        grouped_dataframe.to_csv(
            f'{self.INPUT_PATH}/number-of-therapist-per-org.csv',
            header=False,
            index=False
        )


class TherapistInteractionProcessor:
    INPUT_PATH = 'tmp/input'

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
        dataframe = dataframe.drop_duplicates(
            subset=['therapist_id', 'interaction_date'],
            keep='last'
        )

        # Step 4 - Executes every task from the previous steps.
        cleaned_dataframe = dataframe.compute()

        # Step 5 - Export results into multiple CSV file!
        is_exists = os.path.exists(self.INPUT_PATH)

        if not is_exists:
            os.makedirs(self.INPUT_PATH)

        cleaned_dataframe.to_csv(
            f'{self.INPUT_PATH}/thers-interactions.csv',
            header=False,
            index=False
        )


if __name__ == '__main__':
    configure_logging()

    TherapistProcessor()
    TherapistInteractionProcessor()
