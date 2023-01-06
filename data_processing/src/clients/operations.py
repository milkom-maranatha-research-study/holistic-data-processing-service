import logging
import pandas as pd

from dask import dataframe as dask_dataframe
from typing import Dict, List

from data_processing import settings

from data_processing.src.clients.api import (
    InteractionAPI,
    TotalTherapistAPI,
    TherapistAPI,
    TherapistRateAPI
)
from data_processing.src.clients.mappers import (
    TotalTherapistMapper,
    TherapistRateMapper,
)


logger = logging.getLogger(__name__)


ORG_DIR = 'by-org'
APP_DIR = 'by-app'

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
        Download data from Backend if the Developer Mode is off.

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
        Download data from Backend if the Developer Mode is off.

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

    def upsert(self, total_therapists: List[Dict]) -> None:
        """
        Create or update total therapists in NiceDay.
        """
        self.api.upsert(total_therapists)

    def upsert_by_org(self, org_id: int, total_therapists: List[Dict]) -> None:
        """
        Create or update total therapists in the Organization ID.
        """
        self.api.upsert_by_org(org_id, total_therapists)

    def get_org_weekly_therapists_map(self) -> Dict:
        """
        Returns map of weekly total therapists in the Organization.
        """

        logger.info("Importing weekly total therapists data from disk...")

        path = f'{OUTPUT_ACTIVE_THER_PATH}/{ORG_DIR}/weekly'

        dataframe = pd.read_csv(
            f'{path}/{WEEKLY_ACTIVE_THER_FILENAME}.csv',
            sep='\t',
            header=None
        )

        logger.info("Converting weekly total therapist objects into a dictionary...")
        return self.mapper.to_org_total_thers_map(dataframe, 'weekly')

    def get_org_monthly_therapists_map(self) -> Dict:
        """
        Returns map of monthly total therapists in the Organization.
        """

        logger.info("Importing monthly total therapists data from disk...")

        path = f'{OUTPUT_ACTIVE_THER_PATH}/{ORG_DIR}/monthly'

        dataframe = pd.read_csv(
            f'{path}/{MONTHLY_ACTIVE_THER_FILENAME}.csv',
            sep='\t',
            header=None
        )

        logger.info("Converting monthly total therapist objects into a dictionary...")
        return self.mapper.to_org_total_thers_map(dataframe, 'monthly')

    def get_org_yearly_therapists_map(self) -> Dict:
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
        return self.mapper.to_org_total_thers_map(dataframe, 'yearly')

    def get_nd_alltime_therapists(self) -> List[Dict]:
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
        return self.mapper.to_nd_total_thers(dataframe, 'alltime')

    def get_nd_weekly_therapists(self) -> List[Dict]:
        """
        Returns a list of weekly total therapists in NiceDay.
        """

        logger.info("Importing weekly total therapists data from disk...")

        path = f'{OUTPUT_ACTIVE_THER_PATH}/{APP_DIR}/weekly'

        dataframe = pd.read_csv(
            f'{path}/{WEEKLY_ACTIVE_THER_FILENAME}.csv',
            sep='\t',
            header=None
        )

        logger.info("Converting weekly total therapist objects into a dictionary...")
        return self.mapper.to_nd_total_thers(dataframe, 'weekly')

    def get_nd_monthly_therapists(self) -> List[Dict]:
        """
        Returns a list of monthly total therapists in NiceDay.
        """

        logger.info("Importing monthly total therapists data from disk...")

        path = f'{OUTPUT_ACTIVE_THER_PATH}/{APP_DIR}/monthly'

        dataframe = pd.read_csv(
            f'{path}/{MONTHLY_ACTIVE_THER_FILENAME}.csv',
            sep='\t',
            header=None
        )

        logger.info("Converting monthly total therapist objects into a dictionary...")
        return self.mapper.to_nd_total_thers(dataframe, 'monthly')

    def get_nd_yearly_therapists(self) -> List[Dict]:
        """
        Returns a list of yearly total therapists in NiceDay.
        """

        logger.info("Importing yearly total therapists data from disk...")

        path = f'{OUTPUT_ACTIVE_THER_PATH}/{APP_DIR}/yearly'

        dataframe = pd.read_csv(
            f'{path}/{YEARLY_ACTIVE_THER_FILENAME}.csv',
            sep='\t',
            header=None
        )

        logger.info("Converting yearly total therapist objects into a dictionary...")
        return self.mapper.to_nd_total_thers(dataframe, 'yearly')


class TherapistRateBackendOperation:

    def __init__(self) -> None:
        self.api = TherapistRateAPI()
        self.mapper = TherapistRateMapper()

    def upsert(self, total_therapists: List[Dict]) -> None:
        """
        Create or update therapists' rates in NiceDay.
        """
        self.api.upsert(total_therapists)

    def upsert_by_org(self, org_id: int, total_therapists: List[Dict]) -> None:
        """
        Create or update therapists' rates in the Organization ID.
        """
        self.api.upsert_by_org(org_id, total_therapists)

    def get_org_weekly_rates_map(self) -> Dict:
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
        return self.mapper.to_org_rates_map(dataframe, 'weekly')

    def get_org_monthly_rates_map(self) -> Dict:
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
        return self.mapper.to_org_rates_map(dataframe, 'monthly')

    def get_org_yearly_rates_map(self) -> Dict:
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
        return self.mapper.to_org_rates_map(dataframe, 'yearly')

    def get_nd_weekly_rates(self) -> List[Dict]:
        """
        Returns a list of weekly therapists' rates in NiceDay.
        """

        logger.info("Importing weekly therapists' rates data from disk...")

        path = f'{OUTPUT_RATE_PATH}/{APP_DIR}/weekly'

        dataframe = pd.read_csv(
            f'{path}/{WEEKLY_RATE_FILENAME}.csv',
            sep=',',
            header=None
        )

        logger.info("Converting weekly therapist's rate objects into a dictionary...")
        return self.mapper.to_nd_rates(dataframe, 'weekly')

    def get_nd_monthly_rates(self) -> List[Dict]:
        """
        Returns a list of monthly therapists' rates in NiceDay.
        """

        logger.info("Importing monthly therapists' rates data from disk...")

        path = f'{OUTPUT_RATE_PATH}/{APP_DIR}/monthly'

        dataframe = pd.read_csv(
            f'{path}/{MONTHLY_RATE_FILENAME}.csv',
            sep=',',
            header=None
        )

        logger.info("Converting monthly therapist's rate objects into a dictionary...")
        return self.mapper.to_nd_rates(dataframe, 'monthly')

    def get_nd_yearly_rates(self) -> List[Dict]:
        """
        Returns a list of yearly therapists' rates in NiceDay.
        """

        logger.info("Importing yearly therapists' rates data from disk...")

        path = f'{OUTPUT_RATE_PATH}/{APP_DIR}/yearly'

        dataframe = pd.read_csv(
            f'{path}/{YEARLY_RATE_FILENAME}.csv',
            sep=',',
            header=None
        )

        logger.info("Converting yearly therapist's objects into a dictionary...")
        return self.mapper.to_nd_rates(dataframe, 'yearly')


class TotalTherapistVisualizationOperation:

    def __init__(self) -> None:
        self.api = TotalTherapistAPI()

    @property
    def data(self) -> pd.DataFrame:
        """
        Returns the Interaction dask's dataframe.
        """

        assert hasattr(self, '_df'), (
            'Dask dataframe is not available!\n'
            'You must call `.collect_data()` first.'
        )

        return self._df

    def collect_data(self) -> None:
        """
        Download data from Backend if the Developer Mode is off.

        Otherwise, we load the temporary CSV file from disk.
        """

        logger.info("Collecting Interaction data from Backend or importing from disk...")

        if not settings.DEV_MODE:
            self.api.download_data(format='csv')

        self._df = pd.read_csv(
            self.api._BE_TOTAL_THERAPISTS_FILE,
            dtype={
                'organization_id': 'Int64',
                'type': str,
                'period_type': str,
                'start_date': str,
                'end_date': str,
                'value': 'Int64',
            },
            parse_dates=['start_date', 'end_date']
        )


class TherapistRateVisualizationOperation:

    def __init__(self) -> None:
        self.api = TherapistRateAPI()

    @property
    def data(self) -> pd.DataFrame:
        """
        Returns the Interaction dask's dataframe.
        """

        assert hasattr(self, '_df'), (
            'Dask dataframe is not available!\n'
            'You must call `.collect_data()` first.'
        )

        return self._df

    def collect_data(self) -> None:
        """
        Download data from Backend if the Developer Mode is off.

        Otherwise, we load the temporary CSV file from disk.
        """

        logger.info("Collecting Interaction data from Backend or importing from disk...")

        if not settings.DEV_MODE:
            self.api.download_data(format='csv')

        self._df = pd.read_csv(
            self.api._BE_THERAPISTS_RATES_FILE,
            dtype={
                'organization_id': 'Int64',
                'type': str,
                'period_type': str,
                'start_date': str,
                'end_date': str,
                'rate_value': 'Float64',
            },
            parse_dates=['start_date', 'end_date']
        )
