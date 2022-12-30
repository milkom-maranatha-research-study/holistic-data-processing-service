import logging
import pandas as pd

from dask import dataframe as dask_dataframe
from typing import Dict, List

from data_processor import settings

from data_processor.src.clients.api import (
    AllTimeNumOfTherapistAPI,
    InteractionAPI,
    NumOfTherapistAPI,
    TherapistAPI,
    OrganizationRateAPI
)
from data_processor.src.clients.mappers import (
    AllTimeNumOfTherapistMapper,
    NumOfTherapistMapper,
    OrganizationRateMapper,
)


logger = logging.getLogger(__name__)


ORG_ACTIVE_THERS_OUTPUT_PATH = 'output/org/active-ther-aggregate'

ORG_ALLTIME_ACTIVE_THERS_FILENAME = 'org-active-ther-alltime-aggregate'
ORG_WEEKLY_ACTIVE_THERS_FILENAME = 'org-active-ther-weekly-aggregate'
ORG_MONTHLY_ACTIVE_THERS_FILENAME = 'org-active-ther-monthly-aggregate'
ORG_YEARLY_ACTIVE_THERS_FILENAME = 'org-active-ther-yearly-aggregate'

ORG_RATE_OUTPUT_PATH = 'output/org/rate'
ORG_WEEKLY_RATE_FILENAME = 'org-weekly-rate'
ORG_MONTHLY_RATE_FILENAME = 'org-monthly-rate'
ORG_YEARLY_RATE_FILENAME = 'org-yearly-rate'


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
            f'{ORG_ACTIVE_THERS_OUTPUT_PATH}/{ORG_WEEKLY_ACTIVE_THERS_FILENAME}.csv',
            sep='\t',
            header=None
        )

        logger.info("Converting number of therapist weekly objects into a dictionary...")
        return self.mapper.to_num_of_thers_map(dataframe, 'weekly')

    def _get_monthly_therapists_map(self) -> Dict:
        """
        Returns map of active/inactive therapists per Organization.
        """

        logger.info("Importing active/inactive therapist monthly data from disk...")

        dataframe = pd.read_csv(
            f'{ORG_ACTIVE_THERS_OUTPUT_PATH}/{ORG_MONTHLY_ACTIVE_THERS_FILENAME}.csv',
            sep='\t',
            header=None
        )

        logger.info("Converting number of therapist monthly objects into a dictionary...")
        return self.mapper.to_num_of_thers_map(dataframe, 'monthly')

    def _get_yearly_therapists_map(self) -> Dict:
        """
        Returns map of active/inactive therapists per Organization.
        """

        logger.info("Importing active/inactive therapist yearly data from disk...")

        dataframe = pd.read_csv(
            f'{ORG_ACTIVE_THERS_OUTPUT_PATH}/{ORG_YEARLY_ACTIVE_THERS_FILENAME}.csv',
            sep='\t',
            header=None
        )

        logger.info("Converting number of therapist yearly objects into a dictionary...")
        return self.mapper.to_num_of_thers_map(dataframe, 'yearly')


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
            f'{ORG_ACTIVE_THERS_OUTPUT_PATH}/{ORG_ALLTIME_ACTIVE_THERS_FILENAME}.csv',
            sep='\t',
            header=None
        )

        logger.info("Converting number of therapist all-time objects into a list...")
        return self.mapper.to_num_of_thers_map(dataframe)


class OrganizationRateBackendOperation:

    def __init__(self) -> None:
        self.api = OrganizationRateAPI()
        self.mapper = OrganizationRateMapper()

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

        rates_map = self._get_weekly_rates_map()

        for org_id, rates in rates_map.items():
            self.api.upsert(org_id, rates)

    def _sync_back_monthly_data(self) -> None:
        """
        Synchronize monthly active/inactive therapists in the Organization
        back to the Backend service.
        """

        rates_map = self._get_monthly_rates_map()

        for org_id, rates in rates_map.items():
            self.api.upsert(org_id, rates)

    def _sync_back_yearly_data(self) -> None:
        """
        Synchronize yearly active/inactive therapists in the Organization
        back to the Backend service.
        """

        rates_map = self._get_yearly_rates_map()

        for org_id, rates in rates_map.items():
            self.api.upsert(org_id, rates)

    def _get_weekly_rates_map(self) -> Dict:
        """
        Returns map of active/inactive therapists per Organization.
        """

        logger.info("Importing active/inactive therapist weekly data from disk...")

        dataframe = pd.read_csv(
            f'{ORG_RATE_OUTPUT_PATH}/{ORG_WEEKLY_RATE_FILENAME}.csv',
            sep=',',
            header=None
        )

        logger.info("Converting weekly rates objects into a dictionary...")
        return self.mapper.to_rates_map(dataframe, 'weekly')

    def _get_monthly_rates_map(self) -> Dict:
        """
        Returns map of active/inactive therapists per Organization.
        """

        logger.info("Importing active/inactive therapist monthly data from disk...")

        dataframe = pd.read_csv(
            f'{ORG_RATE_OUTPUT_PATH}/{ORG_MONTHLY_RATE_FILENAME}.csv',
            sep=',',
            header=None
        )

        logger.info("Converting monthly rates objects into a dictionary...")
        return self.mapper.to_rates_map(dataframe, 'monthly')

    def _get_yearly_rates_map(self) -> Dict:
        """
        Returns map of active/inactive therapists per Organization.
        """

        logger.info("Importing active/inactive therapist yearly data from disk...")

        dataframe = pd.read_csv(
            f'{ORG_RATE_OUTPUT_PATH}/{ORG_YEARLY_RATE_FILENAME}.csv',
            sep=',',
            header=None
        )

        logger.info("Converting yearly rates objects into a dictionary...")
        return self.mapper.to_rates_map(dataframe, 'yearly')
