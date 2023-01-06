import logging

from datetime import datetime

from data_processing.src.clients.operations import (
    TotalTherapistBackendOperation,
    TherapistRateBackendOperation,
)
from data_processing.src.helpers import print_time_duration
from data_processing.settings import configure_logging


logger = logging.getLogger(__name__)


class SyncBack:

    def __init__(self) -> None:
        self.total_ther_operation = TotalTherapistBackendOperation()
        self.therapist_rate_operation = TherapistRateBackendOperation()

        # Runs sync back operation
        process_start_at = datetime.now()

        self.total_ther_operation.sync_back()
        self.therapist_rate_operation.sync_back()

        process_end_at = datetime.now()
        print_time_duration("Sync back total therapists and therapists' rates", process_start_at, process_end_at)


if __name__ == '__main__':
    configure_logging()

    SyncBack()
