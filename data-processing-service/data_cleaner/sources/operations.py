import logging
import settings

from typing import List

from sources.clients import TherapistInteractionAPI
from sources.dateutils import DateUtil


logger = logging.getLogger(__name__)


class TherapistInteractionBackendOperation:
    dateutil = DateUtil()

    def __init__(self) -> None:
        self.api = TherapistInteractionAPI()

    def collect_data(self) -> None:
        """
        Download and validates data from Backend if the Developer Mode is off.

        Otherwise, we load the temporary CSV file from disk.
        """

        logger.info("Collecting data from Backend or importing from disk...")

        if not settings.DEV_MODE:
            self.api.download_data(format='csv')

        # data = csv_to_list(self.api._BE_THER_INTERACTIONS_FILE)

    def _validate(self, data: List[List]) -> List[List]:
        """
        Validates downloaded CSV `data` and removes the header's row from it.

        Returns the validated CSV data.
        """

        if len(data) < 1:
            raise ValueError(
                {self.api._BE_THER_INTERACTIONS_FILE: 'The CSV File is empty or invalid.'}
            )

        headers = data.pop(0)
        expected_headers = ['therapist_id', 'interaction_date', 'therapist_chat_count', 'call_count']

        if set(headers) != set(expected_headers):
            raise ValueError(
                {self.api._BE_THER_INTERACTIONS_FILE: 'The headers of CSV File has changed!'}
            )

        return data
