import logging
import os

from settings import configure_logging
from sources.operations import TherapistInteractionBackendOperation


logger = logging.getLogger(__name__)


class TherapistInteractionProcessor:
    INPUT_PATH = 'tmp/input'

    def __init__(self) -> None:
        self.backend_therapist_interaction = TherapistInteractionBackendOperation()

        self.process_data()

    def process_data(self) -> None:
        """
        Process therapists's interaction data from Backend and export it into multiple CSV files.
        """

        # Stage 1 - Collect therapists' interaction data from Backend.
        self.backend_therapist_interaction.collect_data()
        dataframe = self.backend_therapist_interaction.data

        # Stage 2 - Excludes therapists' interaction rows that doesn't have the Organization ID
        # * We consider those rows as dirty data, because every therapist-user must be a member of an Organization.
        dataframe = dataframe.dropna(subset=['organization_id'])

        # Stage 3 - Excludes therapists' interaction rows that has `chat_count` <= 1 and `call_count` < 1.
        # * We assumes that:
        # * a. Therapists' interaction is valid when they send a chat to their clients more than once.
        # *    It means they replied to the client's chat message. OR
        # * b. Therapists' interaction is valid when they have a call with the client at least once.
        # *    It means the therapist and the client talked to each other.
        dataframe = dataframe[(dataframe['chat_count'] > 1) | (dataframe['call_count'] >= 1)]

        # Stage 4 - Distinct rows based on the therapist ID and interaction date.
        # * We assumes that if the therapist interacts with multiple clients in the same day, we only need to pick one.
        # * It's sufficient (for now) to tells that therapist is active on that day.
        dataframe = dataframe.drop_duplicates(subset=['therapist_id', 'interaction_date'], keep='last')

        # Stage 5 - Executes every task from the previous stages.
        dataframe = dataframe.compute()

        # Final Stage - Export results into multiple CSV file!
        self._create_input_dir()
        dataframe.to_csv(f'{self.INPUT_PATH}/thers-interactions.csv', header=False, index=False)

    def _create_input_dir(self):
        """
        Create input directory if it doesn't exists.
        """

        is_exists = os.path.exists(self.INPUT_PATH)

        if not is_exists:
            os.makedirs(self.INPUT_PATH)


if __name__ == '__main__':
    configure_logging()

    TherapistInteractionProcessor()
