from airflow.decorators import dag, task
from datetime import datetime

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig

@dag(
    start_date=datetime(2023, 6, 8),
    schedule='30 * * * *',
    catchup=False,
    tags=['dbt'],
)


def dbt():
    transform = DbtTaskGroup(
            group_id='transform',
            project_config=DBT_PROJECT_CONFIG,
            profile_config=DBT_CONFIG,
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                select=['path:models/transform']
            )
        )

dbt()