import logging

from datetime import datetime

from helpers import print_time_duration
from settings import configure_logging
from targets.operations import (
    NumOfTherapistBackendOperation,
)


logger = logging.getLogger(__name__)


class SyncBackNumOfTherapist:

    def __init__(self) -> None:
        self.num_of_therapist_operation = NumOfTherapistBackendOperation()

        # Runs data processor
        process_start_at = datetime.now()

        self._sync_back()

        process_end_at = datetime.now()
        print_time_duration("Sync back number of therapists", process_start_at, process_end_at)

    def _sync_back(self) -> None:
        """
        Synchronize active/inactive therapists in the Organization
        back to the Backend service.
        """
        self.num_of_therapist_operation.sync_back_weekly_data()
        self.num_of_therapist_operation.sync_back_monthly_data()


if __name__ == '__main__':
    configure_logging()

    SyncBackNumOfTherapist()
