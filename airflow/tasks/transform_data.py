from airflow.decorators import task, branch_task
from airflow.models import DagRun

@task.branch
def select_aggregation_tasks(table_name: str, **kwargs):
    """Chooses which of the aggregation tasks to run: The last 10, 60 minutes of 24 hour option.
    
    Args:
        table_name (string): The table that should be used to get task IDs.
        kwargs: Airflow's context dictionary.
    """
    num_runs = len(DagRun.find(dag_id=kwargs["run_id"]))
    print(num_runs)
    if num_runs % 144 == 0:
        return [f"Transform_Data.aggregate_{table_name}_data_over_one_day", 
                f"Transform_Data.aggregate_{table_name}_data_over_one_hour",
                f"Transform_Data.aggregate_{table_name}_data_over_ten_minutes"]
    elif num_runs % 6 == 0:
        return [f"Transform_Data.aggregate_{table_name}_data_over_one_hour",
                f"Transform_Data.aggregate_{table_name}_data_over_ten_minutes"]
    else: 
        return f"Transform_Data.aggregate_{table_name}_data_over_ten_minutes"
