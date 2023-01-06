import logging

from datetime import datetime

from data_processing.src.clients.operations import (
    TotalTherapistBackendOperation,
    TherapistRateBackendOperation,
)
from data_processing.src.helpers import print_time_duration
from data_processing.settings import configure_logging


logger = logging.getLogger(__name__)


class TotalTherapistSynchronizer:

    def __init__(self) -> None:
        self.operation = TotalTherapistBackendOperation()

        self._sync_back_org_weekly_data()
        self._sync_back_org_monthly_data()
        self._sync_back_org_yearly_data()

        self._sync_back_nd_alltime_data()
        self._sync_back_nd_weekly_data()
        self._sync_back_nd_monthly_data()
        self._sync_back_nd_yearly_data()

    def _sync_back_org_weekly_data(self) -> None:
        """
        Synchronize weekly total therapists in the Organization
        back to the Backend service.
        """

        total_thers_map = self.operation.get_org_weekly_therapists_map()

        for org_id, total_thers in total_thers_map.items():
            self.operation.upsert_by_org(org_id, total_thers)

    def _sync_back_org_monthly_data(self) -> None:
        """
        Synchronize monthly total therapists in the Organization
        back to the Backend service.
        """

        total_thers_map = self.operation.get_org_monthly_therapists_map()

        for org_id, total_thers in total_thers_map.items():
            self.operation.upsert_by_org(org_id, total_thers)

    def _sync_back_org_yearly_data(self) -> None:
        """
        Synchronize yearly total therapists in the Organization
        back to the Backend service.
        """

        total_thers_map = self.operation.get_org_yearly_therapists_map()

        for org_id, total_thers in total_thers_map.items():
            self.operation.upsert_by_org(org_id, total_thers)

    def _sync_back_nd_alltime_data(self) -> None:
        """
        Synchronize the total all therapists in NiceDay
        back to the Backend service.
        """

        total_thers = self.operation.get_nd_alltime_therapists()
        self.operation.upsert(total_thers)

    def _sync_back_nd_weekly_data(self) -> None:
        """
        Synchronize weekly total therapists in NiceDay
        back to the Backend service.
        """

        total_thers = self.operation.get_nd_weekly_therapists()
        self.operation.upsert(total_thers)

    def _sync_back_nd_monthly_data(self) -> None:
        """
        Synchronize monthly total therapists in NiceDay
        back to the Backend service.
        """

        total_thers = self.operation.get_nd_monthly_therapists()
        self.operation.upsert(total_thers)

    def _sync_back_nd_yearly_data(self) -> None:
        """
        Synchronize yearly total therapists in NiceDay
        back to the Backend service.
        """

        total_thers = self.operation.get_nd_yearly_therapists()
        self.operation.upsert(total_thers)


class TherapistRateSynchronizer:

    def __init__(self) -> None:
        self.operation = TherapistRateBackendOperation()

        self._sync_back_org_weekly_data()
        self._sync_back_org_monthly_data()
        self._sync_back_org_yearly_data()

        self._sync_back_nd_weekly_data()
        self._sync_back_nd_monthly_data()
        self._sync_back_nd_yearly_data()

    def _sync_back_org_weekly_data(self) -> None:
        """
        Synchronize weekly therapists' rates in the Organization
        back to the Backend service.
        """

        rates_map = self.operation.get_org_weekly_rates_map()

        for org_id, rates in rates_map.items():
            self.operation.upsert_by_org(org_id, rates)

    def _sync_back_org_monthly_data(self) -> None:
        """
        Synchronize monthly therapists' rates in the Organization
        back to the Backend service.
        """

        rates_map = self.operation.get_org_monthly_rates_map()

        for org_id, rates in rates_map.items():
            self.operation.upsert_by_org(org_id, rates)

    def _sync_back_org_yearly_data(self) -> None:
        """
        Synchronize yearly therapists' rates in the Organization
        back to the Backend service.
        """

        rates_map = self.operation.get_org_yearly_rates_map()

        for org_id, rates in rates_map.items():
            self.operation.upsert_by_org(org_id, rates)

    def _sync_back_nd_weekly_data(self) -> None:
        """
        Synchronize weekly therapists' rates in NiceDay
        back to the Backend service.
        """

        rates = self.operation.get_nd_weekly_rates()
        self.operation.upsert(rates)

    def _sync_back_nd_monthly_data(self) -> None:
        """
        Synchronize monthly therapists' rates in NiceDay
        back to the Backend service.
        """

        rates = self.operation.get_nd_monthly_rates()
        self.operation.upsert(rates)

    def _sync_back_nd_yearly_data(self) -> None:
        """
        Synchronize yearly therapists' rates in NiceDay
        back to the Backend service.
        """

        rates = self.operation.get_nd_yearly_rates()
        self.operation.upsert(rates)


if __name__ == '__main__':
    configure_logging()

    # Runs sync back operation
    process_start_at = datetime.now()

    TotalTherapistSynchronizer()
    TherapistRateSynchronizer()

    process_end_at = datetime.now()
    tag = "Sync back total therapists and therapists' rates"
    print_time_duration(tag, process_start_at, process_end_at)
