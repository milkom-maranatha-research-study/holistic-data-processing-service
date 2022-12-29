import logging
import pandas as pd
import settings

from dask import dataframe as dask_dataframe
from typing import Dict, List

from clients.api import (
    AllTimeNumOfTherapistAPI,
    InteractionAPI,
    TherapistAPI,
    NumOfTherapistAPI,
)
from clients.mappers import (
    AllTimeNumOfTherapistMapper,
    NumOfTherapistMapper,
)


logger = logging.getLogger(__name__)


OUTPUT_ACTIVE_INACTIVE_DIRECTORY = 'output/active-inactive'

ALL_TIME_AGGREGATE_FILENAME = 'alltime-aggregate.csv'
WEEKLY_AGGREGATE_FILENAME = 'weekly-aggregate.csv'
MONTHLY_AGGREGATE_FILENAME = 'monthly-aggregate.csv'
YEARLY_AGGREGATE_FILENAME = 'yearly-aggregate.csv'


class TherapistBackendOperation:

    def __init__(self) -> None:
        self.api = TherapistAPI()

    @property
    def data(self) -> dask_dataframe:
        """
        Returns the Therapist dask's dataframe.
        """

        assert hasattr(self, '_ddf'), (
            'Dask dataframe is not available!\n'
            'You must call `.collect_data()` first.'
        )

        return self._ddf

    def collect_data(self) -> None:
        """
        Download and validates data from Backend if the Developer Mode is off.

        Otherwise, we load the temporary CSV file from disk.
        """

        logger.info("Collecting Therapist data from Backend or importing from disk...")

        if not settings.DEV_MODE:
            self.api.download_data(format='csv')

        self._ddf = dask_dataframe.read_csv(
            self.api._BE_THERAPISTS_FILE,
            dtype={
                'id': str,
                'organization_id': 'Int64',
                'date_joined': str
            },
            parse_dates=['date_joined']
        )


class InteractionBackendOperation:

    def __init__(self) -> None:
        self.api = InteractionAPI()

    @property
    def data(self) -> dask_dataframe:
        """
        Returns the Interaction dask's dataframe.
        """

        assert hasattr(self, '_ddf'), (
            'Dask dataframe is not available!\n'
            'You must call `.collect_data()` first.'
        )

        return self._ddf

    def collect_data(self) -> None:
        """
        Download and validates data from Backend if the Developer Mode is off.

        Otherwise, we load the temporary CSV file from disk.
        """

        logger.info("Collecting Interaction data from Backend or importing from disk...")

        if not settings.DEV_MODE:
            self.api.download_data(format='csv')

        self._ddf = dask_dataframe.read_csv(
            self.api._BE_THER_INTERACTIONS_FILE,
            dtype={
                'therapist_id': str,
                'interaction_date': str,
                'counter': 'Int64',
                'chat_count': 'Int64',
                'call_count': 'Int64',
                'organization_id': 'Int64',
                'organization_date_joined': str
            },
            parse_dates=['interaction_date', 'organization_date_joined']
        )


class NumOfTherapistBackendOperation:

    def __init__(self) -> None:
        self.api = NumOfTherapistAPI()
        self.mapper = NumOfTherapistMapper()

    def sync_back(self) -> None:
        """
        Synchronize every active/inactive therapists in the Organization
        back to the Backend service.
        """
        self._sync_back_weekly_data()
        self._sync_back_monthly_data()
        self._sync_back_yearly_data()

    def _sync_back_weekly_data(self) -> None:
        """
        Synchronize weekly active/inactive therapists in the Organization
        back to the Backend service.
        """

        num_of_thers_map = self._get_weekly_therapists_map()

        for org_id, num_of_thers in num_of_thers_map.items():
            self.api.upsert(org_id, num_of_thers)

    def _sync_back_monthly_data(self) -> None:
        """
        Synchronize monthly active/inactive therapists in the Organization
        back to the Backend service.
        """

        num_of_thers_map = self._get_monthly_therapists_map()

        for org_id, num_of_thers in num_of_thers_map.items():
            self.api.upsert(org_id, num_of_thers)

    def _sync_back_yearly_data(self) -> None:
        """
        Synchronize yearly active/inactive therapists in the Organization
        back to the Backend service.
        """

        num_of_thers_map = self._get_yearly_therapists_map()

        for org_id, num_of_thers in num_of_thers_map.items():
            self.api.upsert(org_id, num_of_thers)

    def _get_weekly_therapists_map(self) -> Dict:
        """
        Returns map of active/inactive therapists per Organization.
        """

        logger.info("Importing active/inactive therapist weekly data from disk...")

        dataframe = pd.read_csv(
            f'{OUTPUT_ACTIVE_INACTIVE_DIRECTORY}/{WEEKLY_AGGREGATE_FILENAME}',
            sep='\t',
            header=None
        )

        logger.info("Converting number of therapist weekly objects into a dictionary...")
        return self.mapper.to_weekly_therapists_map(dataframe)

    def _get_monthly_therapists_map(self) -> Dict:
        """
        Returns map of active/inactive therapists per Organization.
        """

        logger.info("Importing active/inactive therapist monthly data from disk...")

        dataframe = pd.read_csv(
            f'{OUTPUT_ACTIVE_INACTIVE_DIRECTORY}/{MONTHLY_AGGREGATE_FILENAME}',
            sep='\t',
            header=None
        )

        logger.info("Converting number of therapist monthly objects into a dictionary...")
        return self.mapper.to_monthly_therapists_map(dataframe)

    def _get_yearly_therapists_map(self) -> Dict:
        """
        Returns map of active/inactive therapists per Organization.
        """

        logger.info("Importing active/inactive therapist yearly data from disk...")

        dataframe = pd.read_csv(
            f'{OUTPUT_ACTIVE_INACTIVE_DIRECTORY}/{YEARLY_AGGREGATE_FILENAME}',
            sep='\t',
            header=None
        )

        logger.info("Converting number of therapist yearly objects into a dictionary...")
        return self.mapper.to_yearly_therapists_map(dataframe)


class AllTimeNumOfTherapistBackendOperation:

    def __init__(self) -> None:
        self.api = AllTimeNumOfTherapistAPI()
        self.mapper = AllTimeNumOfTherapistMapper()

    def sync_back(self) -> None:
        """
        Synchronize all-time active/inactive therapists in NiceDay
        back to the Backend service.
        """

        num_of_thers = self._get_all_time_therapists()

        for num_of_ther in num_of_thers:
            self.api.upsert(num_of_ther)

    def _get_all_time_therapists(self) -> List[Dict]:
        """
        Returns a list of all time active/inactive therapists.
        """

        logger.info("Importing all-time active/inactive therapists from disk...")

        dataframe = pd.read_csv(
            f'{OUTPUT_ACTIVE_INACTIVE_DIRECTORY}/{ALL_TIME_AGGREGATE_FILENAME}',
            sep='\t',
            header=None
        )

        logger.info("Converting number of therapist all-time objects into a list...")
        return self.mapper.to_all_time_therapists(dataframe)
