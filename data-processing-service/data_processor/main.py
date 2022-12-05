import logging

from settings import configure_logging
from sources.operations import TherapistInteractionBackendOperation


logger = logging.getLogger(__name__)


class TherapistInteractionProcessor:

    def __init__(self) -> None:
        self.backend_therapist_interaction = TherapistInteractionBackendOperation()
        self.backend_therapist_interaction.collect_data()

        self.process_data()

    def process_data(self) -> None:
        pass


if __name__ == '__main__':
    configure_logging()

    TherapistInteractionProcessor()
