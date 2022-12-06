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
    
    @property
    def data(self) -> dask_dataframe:
        """
        Returns the Therapist Interaction dask's dataframe.
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

        logger.info("Collecting data from Backend or importing from disk...")

        if not settings.DEV_MODE:
            self.api.download_data(format='csv')

        self._ddf = dask_dataframe.read_csv(
            self.api._BE_THER_INTERACTIONS_FILE,
            dtype={
                'interaction_id': 'Int64',
                'therapist_id': str,
                'chat_count': 'Int64',
                'call_count': 'Int64',
                'interaction_date': str,
                'organization_id': 'Int64',
                'organization_date_joined': str
            },
            parse_dates=['interaction_date', 'organization_date_joined']
        )
