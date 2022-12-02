import logging

from settings import configure_logging
from sources.operations import TherapistInteractionBackendOperation


logger = logging.getLogger(__name__)


class TherapistInteractionCleaner:

    def __init__(self) -> None:
        self.backend_therapist_interaction = TherapistInteractionBackendOperation()
        self.backend_therapist_interaction.collect_data()

    def clean(self) -> None:
        pass


if __name__ == '__main__':
    configure_logging()

    TherapistInteractionCleaner().clean()
