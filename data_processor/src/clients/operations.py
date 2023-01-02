import logging
import pandas as pd

from dask import dataframe as dask_dataframe
from typing import Dict, List

from data_processor import settings

from data_processor.src.clients.api import (
    TotalAllTherapistAPI,
    InteractionAPI,
    TotalTherapistAPI,
    TherapistAPI,
    TherapistRateAPI
)
from data_processor.src.clients.mappers import (
    TotalAllTherapistMapper,
    TotalTherapistMapper,
    TherapistRateMapper,
)


logger = logging.getLogger(__name__)


ORG_DIR = 'by-org'

OUTPUT_ACTIVE_THER_PATH = 'output/active-ther'

ALLTIME_ACTIVE_THER_FILENAME = 'active-ther-alltime-aggregate'
WEEKLY_ACTIVE_THER_FILENAME = 'active-ther-weekly-aggregate'
MONTHLY_ACTIVE_THER_FILENAME = 'active-ther-monthly-aggregate'
YEARLY_ACTIVE_THER_FILENAME = 'active-ther-yearly-aggregate'

OUTPUT_RATE_PATH = 'output/rate'

WEEKLY_RATE_FILENAME = 'output-weekly-rate'
MONTHLY_RATE_FILENAME = 'output-monthly-rate'
YEARLY_RATE_FILENAME = 'output-yearly-rate'


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


class TotalTherapistBackendOperation:

    def __init__(self) -> None:
        self.api = TotalTherapistAPI()
        self.mapper = TotalTherapistMapper()

    def sync_back(self) -> None:
        """
        Synchronize every total therapists in the Organization
        back to the Backend service.
        """
        self._sync_back_weekly_data()
        self._sync_back_monthly_data()
        self._sync_back_yearly_data()

    def _sync_back_weekly_data(self) -> None:
        """
        Synchronize weekly total therapists in the Organization
        back to the Backend service.
        """

        num_of_thers_map = self._get_weekly_therapists_map()

        for org_id, num_of_thers in num_of_thers_map.items():
            self.api.upsert(org_id, num_of_thers)

    def _sync_back_monthly_data(self) -> None:
        """
        Synchronize monthly total therapists in the Organization
        back to the Backend service.
        """

        num_of_thers_map = self._get_monthly_therapists_map()

        for org_id, num_of_thers in num_of_thers_map.items():
            self.api.upsert(org_id, num_of_thers)

    def _sync_back_yearly_data(self) -> None:
        """
        Synchronize yearly total therapists in the Organization
        back to the Backend service.
        """

        num_of_thers_map = self._get_yearly_therapists_map()

        for org_id, num_of_thers in num_of_thers_map.items():
            self.api.upsert(org_id, num_of_thers)

    def _get_weekly_therapists_map(self) -> Dict:
        """
        Returns map of total therapists in the Organization.
        """

        logger.info("Importing weekly total therapists data from disk...")

        path = f'{OUTPUT_ACTIVE_THER_PATH}/{ORG_DIR}/weekly'

        dataframe = pd.read_csv(
            f'{path}/{WEEKLY_ACTIVE_THER_FILENAME}.csv',
            sep='\t',
            header=None
        )

        logger.info("Converting weekly total therapist objects into a dictionary...")
        return self.mapper.to_total_thers_map(dataframe, 'weekly')

    def _get_monthly_therapists_map(self) -> Dict:
        """
        Returns map of montly total therapists in the Organization.
        """

        logger.info("Importing monthly total therapists data from disk...")

        path = f'{OUTPUT_ACTIVE_THER_PATH}/{ORG_DIR}/monthly'

        dataframe = pd.read_csv(
            f'{path}/{MONTHLY_ACTIVE_THER_FILENAME}.csv',
            sep='\t',
            header=None
        )

        logger.info("Converting monthly total therapist objects into a dictionary...")
        return self.mapper.to_total_thers_map(dataframe, 'monthly')

    def _get_yearly_therapists_map(self) -> Dict:
        """
        Returns map of yearly total therapists in the Organization.
        """

        logger.info("Importing yearly total therapists data from disk...")

        path = f'{OUTPUT_ACTIVE_THER_PATH}/{ORG_DIR}/yearly'

        dataframe = pd.read_csv(
            f'{path}/{YEARLY_ACTIVE_THER_FILENAME}.csv',
            sep='\t',
            header=None
        )

        logger.info("Converting yearly total therapist objects into a dictionary...")
        return self.mapper.to_total_thers_map(dataframe, 'yearly')


class TotalAllTherapistBackendOperation:

    def __init__(self) -> None:
        self.api = TotalAllTherapistAPI()
        self.mapper = TotalAllTherapistMapper()

    def sync_back(self) -> None:
        """
        Synchronize the total all therapists in NiceDay
        back to the Backend service.
        """

        num_of_thers = self._get_all_time_therapists()

        for num_of_ther in num_of_thers:
            self.api.upsert(num_of_ther)

    def _get_all_time_therapists(self) -> List[Dict]:
        """
        Returns a list of the total all therapists.
        """

        logger.info("Importing total all therapists from disk...")

        path = f'{OUTPUT_ACTIVE_THER_PATH}/alltime'

        dataframe = pd.read_csv(
            f'{path}/{ALLTIME_ACTIVE_THER_FILENAME}.csv',
            sep='\t',
            header=None
        )

        logger.info("Converting total all therapist's objects into a list...")
        return self.mapper.to_total_thers_map(dataframe)


class TherapistRateBackendOperation:

    def __init__(self) -> None:
        self.api = TherapistRateAPI()
        self.mapper = TherapistRateMapper()

    def sync_back(self) -> None:
        """
        Synchronize every therapists' rates in the Organization
        back to the Backend service.
        """
        self._sync_back_weekly_data()
        self._sync_back_monthly_data()
        self._sync_back_yearly_data()

    def _sync_back_weekly_data(self) -> None:
        """
        Synchronize weekly therapists' rates in the Organization
        back to the Backend service.
        """

        rates_map = self._get_weekly_rates_map()

        for org_id, rates in rates_map.items():
            self.api.upsert(org_id, rates)

    def _sync_back_monthly_data(self) -> None:
        """
        Synchronize monthly therapists' rates in the Organization
        back to the Backend service.
        """

        rates_map = self._get_monthly_rates_map()

        for org_id, rates in rates_map.items():
            self.api.upsert(org_id, rates)

    def _sync_back_yearly_data(self) -> None:
        """
        Synchronize yearly therapists' rates in the Organization
        back to the Backend service.
        """

        rates_map = self._get_yearly_rates_map()

        for org_id, rates in rates_map.items():
            self.api.upsert(org_id, rates)

    def _get_weekly_rates_map(self) -> Dict:
        """
        Returns map of weekly therapists' rates in the Organization.
        """

        logger.info("Importing weekly therapists' rates data from disk...")

        path = f'{OUTPUT_RATE_PATH}/{ORG_DIR}/weekly'

        dataframe = pd.read_csv(
            f'{path}/{WEEKLY_RATE_FILENAME}.csv',
            sep=',',
            header=None
        )

        logger.info("Converting weekly therapist's rate objects into a dictionary...")
        return self.mapper.to_rates_map(dataframe, 'weekly')

    def _get_monthly_rates_map(self) -> Dict:
        """
        Returns map of weekly therapists' rates in the Organization.
        """

        logger.info("Importing monthly therapists' rates data from disk...")

        path = f'{OUTPUT_RATE_PATH}/{ORG_DIR}/monthly'

        dataframe = pd.read_csv(
            f'{path}/{MONTHLY_RATE_FILENAME}.csv',
            sep=',',
            header=None
        )

        logger.info("Converting monthly therapist's rate objects into a dictionary...")
        return self.mapper.to_rates_map(dataframe, 'monthly')

    def _get_yearly_rates_map(self) -> Dict:
        """
        Returns map of yearly therapists' rates in the Organization.
        """

        logger.info("Importing yearly therapists' rates data from disk...")

        path = f'{OUTPUT_RATE_PATH}/{ORG_DIR}/yearly'

        dataframe = pd.read_csv(
            f'{path}/{YEARLY_RATE_FILENAME}.csv',
            sep=',',
            header=None
        )

        logger.info("Converting yearly therapist's objects into a dictionary...")
        return self.mapper.to_rates_map(dataframe, 'yearly')
