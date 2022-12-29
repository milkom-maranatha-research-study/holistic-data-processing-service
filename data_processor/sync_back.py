import logging

from datetime import datetime

from helpers import print_time_duration
from settings import configure_logging
from clients.operations import (
    AllTimeNumOfTherapistBackendOperation,
    NumOfTherapistBackendOperation,
)


logger = logging.getLogger(__name__)


class SyncBack:

    def __init__(self) -> None:
        self.all_time_ther_operation = AllTimeNumOfTherapistBackendOperation()
        self.num_of_ther_operation = NumOfTherapistBackendOperation()

        # Runs data processor
        process_start_at = datetime.now()

        self.all_time_ther_operation.sync_back()
        self.num_of_ther_operation.sync_back()

        process_end_at = datetime.now()
        print_time_duration("Sync back number of therapists", process_start_at, process_end_at)


if __name__ == '__main__':
    configure_logging()

    SyncBack()
