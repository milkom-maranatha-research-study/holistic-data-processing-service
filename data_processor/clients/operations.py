import logging
import pandas as pd
import settings

from dask import dataframe as dask_dataframe
from typing import Dict

from clients.api import (
    InteractionAPI,
    TherapistAPI,
    NumOfTherapistAPI,
)
from clients.mappers import NumOfTherapistMapper


logger = logging.getLogger(__name__)


NUM_OF_THER_WEEKLY_FILENAME = 'active-inactive-weekly-aggregate.csv'
NUM_OF_THER_MONTHLY_FILENAME = 'active-inactive-monthly-aggregate.csv'
NUM_OF_THER_YEARLY_FILENAME = 'active-inactive-yearly-aggregate.csv'


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

    def sync_back_weekly_data(self) -> None:
        """
        Synchronize weekly active/inactive therapists in the Organization
        back to the Backend service.
        """

        num_of_thers_map = self._get_weekly_therapists_map()

        for org_id, num_of_thers in num_of_thers_map.items():
            self.api.upsert(org_id, num_of_thers)

    def sync_back_monthly_data(self) -> None:
        """
        Synchronize monthly active/inactive therapists in the Organization
        back to the Backend service.
        """

        num_of_thers_map = self._get_monthly_therapists_map()

        for org_id, num_of_thers in num_of_thers_map.items():
            self.api.upsert(org_id, num_of_thers)

    def sync_back_yearly_data(self) -> None:
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
            NUM_OF_THER_WEEKLY_FILENAME,
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
            NUM_OF_THER_MONTHLY_FILENAME,
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
            NUM_OF_THER_YEARLY_FILENAME,
            sep='\t',
            header=None
        )

        logger.info("Converting number of therapist yearly objects into a dictionary...")
        return self.mapper.to_yearly_therapists_map(dataframe)