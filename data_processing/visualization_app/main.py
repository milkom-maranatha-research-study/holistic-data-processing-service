import logging
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from datetime import datetime
from os import path, makedirs
from pandas import DataFrame

from data_processing.src.clients.operations import (
    TotalTherapistVisualizationOperation,
    TherapistRateVisualizationOperation,
)
from data_processing.src.helpers import print_time_duration
from data_processing.settings import configure_logging


logger = logging.getLogger(__name__)


ACTIVE_THER_DIR = 'active-ther'
THER_RATE_DIR = 'rate'

WEEKLY_DIR = 'weekly'
MONTHLY_DIR = 'monthly'
YEARLY_DIR = 'yearly'


class DataVisualization:

    def get_bar_insight(
        self,
        x_column_name: str,
        y_column_name: str,
        comparison: str,
        dataframe: DataFrame,
        title: str,
        directory: str
    ) -> None:
        """
        Get insight of the specific `x_column_name` respecting the `y_column_name`
        with that `comparison` column in the given `dataframe` as the Bar Chart and put some `title` on it.

        The method will also saves the generated Bar Chart
        into the output directory.
        """
        # Configures mathplot library
        plt.title(title, fontsize=18, pad=25)
        plt.xticks(rotation=45, ha='right')

        # Converts the value counts of that `column_name` with respect
        # to that `comparison` column
        sns.set_theme(style="whitegrid")
        sns.barplot(data=dataframe, x=x_column_name, y=y_column_name, hue=comparison)

        # Create output directory if it doesn't exists
        base_path = path.abspath(path.dirname(path.dirname(__name__)))
        outputs_path = f'{base_path}/visualization/{directory}'

        if not path.exists(outputs_path):
            makedirs(outputs_path)

        # Saves the plot as PNG file
        filename = title.replace('\n', ' ').replace('/', ' or ')
        plt.savefig(f'{outputs_path}/{filename}.png', bbox_inches='tight')

        # Clear the current figure
        plt.close()


class TotalTherapistDataVisualization(DataVisualization):

    def __init__(self) -> None:
        self.operation = TotalTherapistVisualizationOperation()

        # Runs data visualization
        process_start_at = datetime.now()

        base_path = path.abspath(path.dirname(path.dirname(__name__)))
        dataframe = pd.read_csv(
            f'{base_path}/.be_total_therapists.csv.tmp',
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

        # Only include the rows where the Organization ID is NaN
        dataframe = dataframe[(~(dataframe.organization_id.notnull()))]

        nd_weekly_df = dataframe[(dataframe['period_type'] == 'weekly')]
        nd_monthly_df = dataframe[(dataframe['period_type'] == 'monthly')]
        nd_yearly_df = dataframe[(dataframe['period_type'] == 'yearly')]

        # self._show_nd_weekly_total_thers(nd_weekly_df)
        self._show_nd_monthly_total_thers(nd_monthly_df)
        self._show_nd_yearly_total_thers(nd_yearly_df)

        process_end_at = datetime.now()
        tag = "Total Therapist data visualization"
        print_time_duration(tag, process_start_at, process_end_at)

    def _show_nd_weekly_total_thers(self, dataframe: DataFrame) -> None:
        dataframe = dataframe.assign(period=lambda x: x.start_date.dt.to_period('W'))

        min_year = dataframe.start_date.min().year
        max_year = dataframe.end_date.max().year + 1

        for year in range(min_year, max_year):
            start = pd.Timestamp(year=year, month=1, day=1)
            end = pd.Timestamp(year=year, month=12, day=31)

            dataset = dataframe[
                (dataframe.start_date >= start) &
                (dataframe.end_date <= end)
            ]

            self.get_bar_insight(
                'period',
                'value',
                'type',
                dataset,
                f'Weekly Active/Inactive Therapist at {year}',
                f'{ACTIVE_THER_DIR}/{WEEKLY_DIR}/'
            )

    def _show_nd_monthly_total_thers(self, dataframe: DataFrame) -> None:
        dataframe = dataframe.assign(period=lambda x: x.start_date.dt.to_period('M'))

        min_year = dataframe.start_date.min().year
        max_year = dataframe.end_date.max().year + 1

        for year in range(min_year, max_year):
            start = pd.Timestamp(year=year, month=1, day=1)
            end = pd.Timestamp(year=year, month=12, day=31)

            dataset = dataframe[
                (dataframe.start_date >= start) &
                (dataframe.end_date <= end)
            ]

            self.get_bar_insight(
                'period',
                'value',
                'type',
                dataset,
                f'Monthly Active/Inactive Therapist at {year}',
                f'{ACTIVE_THER_DIR}/{MONTHLY_DIR}/'
            )

    def _show_nd_yearly_total_thers(self, dataframe: DataFrame) -> None:
        dataframe = dataframe.assign(period=lambda x: x.start_date.dt.to_period('Y'))

        min_year = dataframe.start_date.min().year
        max_year = dataframe.end_date.max().year + 1

        self.get_bar_insight(
            'period',
            'value',
            'type',
            dataframe,
            f'Yearly Active/Inactive Therapist\n{min_year} - {max_year}',
            f'{ACTIVE_THER_DIR}/{YEARLY_DIR}/'
        )


class TherapistRateDataVisualization(DataVisualization):

    def __init__(self) -> None:
        self.operation = TherapistRateVisualizationOperation()

        # Runs data visualization
        process_start_at = datetime.now()
    
        base_path = path.abspath(path.dirname(path.dirname(__name__)))
        dataframe = pd.read_csv(
            f'{base_path}/.be_therapists_rates.csv.tmp',
            dtype={
                'organization_id': 'Int64',
                'type': str,
                'period_type': str,
                'start_date': str,
                'end_date': str,
                'value': 'Float64',
            },
            parse_dates=['start_date', 'end_date']
        )

        # Only include the rows where the Organization ID is NaN
        dataframe = dataframe[(~(dataframe.organization_id.notnull()))]

        nd_weekly_df = dataframe[(dataframe['period_type'] == 'weekly')]
        nd_monthly_df = dataframe[(dataframe['period_type'] == 'monthly')]
        nd_yearly_df = dataframe[(dataframe['period_type'] == 'yearly')]

        # self._show_nd_weekly_rates(nd_weekly_df)
        self._show_nd_monthly_rates(nd_monthly_df)
        self._show_nd_yearly_rates(nd_yearly_df)

        process_end_at = datetime.now()
        tag = "Therapist Rate data visualization"
        print_time_duration(tag, process_start_at, process_end_at)

    def _show_nd_weekly_rates(self, dataframe: DataFrame) -> None:
        dataframe = dataframe.assign(period=lambda x: x.start_date.dt.to_period('W'))

        min_year = dataframe.start_date.min().year
        max_year = dataframe.end_date.max().year + 1

        for year in range(min_year, max_year):
            start = pd.Timestamp(year=year, month=1, day=1)
            end = pd.Timestamp(year=year, month=12, day=31)

            dataset = dataframe[
                (dataframe.start_date >= start) &
                (dataframe.end_date <= end)
            ]

            self.get_bar_insight(
                'period',
                'value',
                'type',
                dataset,
                f'Weekly Therapist Rates at {year}',
                f'{THER_RATE_DIR}/{WEEKLY_DIR}/'
            )

    def _show_nd_monthly_rates(self, dataframe: DataFrame) -> None:
        dataframe = dataframe.assign(period=lambda x: x.start_date.dt.to_period('M'))

        min_year = dataframe.start_date.min().year
        max_year = dataframe.end_date.max().year + 1

        for year in range(min_year, max_year):
            start = pd.Timestamp(year=year, month=1, day=1)
            end = pd.Timestamp(year=year, month=12, day=31)

            dataset = dataframe[
                (dataframe.start_date >= start) &
                (dataframe.end_date <= end)
            ]

            self.get_bar_insight(
                'period',
                'value',
                'type',
                dataset,
                f'Monthly Therapist Rates at {year}',
                f'{THER_RATE_DIR}/{MONTHLY_DIR}/'
            )

    def _show_nd_yearly_rates(self, dataframe: DataFrame) -> None:
        dataframe = dataframe.assign(period=lambda x: x.start_date.dt.to_period('Y'))

        min_year = dataframe.start_date.min().year
        max_year = dataframe.end_date.max().year + 1

        self.get_bar_insight(
            'period',
            'value',
            'type',
            dataframe,
            f'Yearly Therapist Rates\n{min_year} - {max_year}',
            f'{THER_RATE_DIR}/{YEARLY_DIR}/'
        )


if __name__ == '__main__':
    configure_logging()

    TotalTherapistDataVisualization()
    TherapistRateDataVisualization()
