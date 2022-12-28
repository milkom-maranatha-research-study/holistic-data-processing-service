import logging
import pandas as pd

from typing import Dict

from targets.clients import NumOfTherapistAPI
from targets.mappers import NumOfTherapistMapper


logger = logging.getLogger(__name__)


NUM_OF_THER_WEEKLY_FILENAME = 'active-inactive-weekly-aggregate.csv'
NUM_OF_THER_MONTHLY_FILENAME = 'active-inactive-monthly-aggregate.csv'
NUM_OF_THER_YEARLY_FILENAME = 'active-inactive-yearly-aggregate.csv'


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
