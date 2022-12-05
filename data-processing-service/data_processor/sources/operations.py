import logging
import settings

from dask import dataframe as dask_dataframe

from sources.clients import TherapistInteractionAPI
from sources.dateutils import DateUtil


logger = logging.getLogger(__name__)


class TherapistInteractionBackendOperation:
    dateutil = DateUtil()

    def __init__(self) -> None:
        self.api = TherapistInteractionAPI()

    def collect_data(self) -> None:
        """
        Download and validates data from Backend if the Developer Mode is off.

        Otherwise, we load the temporary CSV file from disk.
        """

        logger.info("Collecting data from Backend or importing from disk...")

        if not settings.DEV_MODE:
            self.api.download_data(format='csv')

        self.ddf = dask_dataframe.read_csv(self.api._BE_THER_INTERACTIONS_FILE, parse_dates=['interaction_date', 'organization_date_joined'])
