from pathlib import Path
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig


def dbt_transformations_pipeline(parent_group):
    """
    Pipeline to run DBT transformations using Astronomer Cosmos.

    This creates tasks for dbt models with proper dependencies based on the dbt project structure.
    Note: When used with @dag decorator, Cosmos will automatically inherit the DAG context.

    Args:
        parent_group: The parent task group ID for logging and tracking

    Returns:
        DbtTaskGroup: The task group containing all dbt tasks
    """

    # Configure dbt project paths
    DBT_PROJECT_PATH = Path('/opt/dbt')
    DBT_PROFILES_PATH = Path('/opt/dbt')

    # Create dbt task group using Cosmos
    dbt_tg = DbtTaskGroup(
        group_id='dbt_models',
        project_config=ProjectConfig(
            dbt_project_path=str(DBT_PROJECT_PATH),
        ),
        profile_config=ProfileConfig(
            profile_name='ebury',
            target_name='dev',
            profiles_yml_filepath=str(DBT_PROFILES_PATH / 'profiles.yml'),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path='/home/airflow/.local/bin/dbt',
        ),
        render_config=RenderConfig(
            # No select parameter = run all models in the project
            dbt_deps=True,  
        ),
        operator_args={
            'install_deps': True,  
        },
    )

    return dbt_tg
