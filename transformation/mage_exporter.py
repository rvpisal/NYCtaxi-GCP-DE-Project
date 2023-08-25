from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(data, **kwargs) -> None:
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    # since we have our data tables and values in a dictionary, we can loop through it and get table names
    for key, value in data.items():
        table_id = 'de-uber-analytics-ytproject.uber_de_dataset.{0}'.format(key)
        config_path = path.join(get_repo_path(), 'io_config.yaml')
        config_profile = 'default'
        BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            DataFrame(value), #convert the data of the table into dataframe
            table_id,
            if_exists='replace',  # Specify resolution policy if table name already exists
        )
