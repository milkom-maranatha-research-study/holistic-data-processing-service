import logging

from datetime import datetime

from data_processor.src.clients.operations import (
    TotalAllTherapistBackendOperation,
    TotalTherapistBackendOperation,
    TherapistRateBackendOperation,
)
from data_processor.src.helpers import print_time_duration
from data_processor.settings import configure_logging


logger = logging.getLogger(__name__)


class SyncBack:

    def __init__(self) -> None:

        self.total_all_ther_operation = TotalAllTherapistBackendOperation()
        self.total_ther_operation = TotalTherapistBackendOperation()
        self.organization_rates_operation = TherapistRateBackendOperation()

        # Runs sync back operation
        process_start_at = datetime.now()

        self.total_all_ther_operation.sync_back()
        self.total_ther_operation.sync_back()
        self.organization_rates_operation.sync_back()

        process_end_at = datetime.now()
        print_time_duration("Sync back total therapists and organizations' rates", process_start_at, process_end_at)


if __name__ == '__main__':
    configure_logging()

    SyncBack()
