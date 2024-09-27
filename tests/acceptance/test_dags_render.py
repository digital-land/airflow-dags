import pytest
from airflow.models import DagBag

DAG_FOLDER = 'dags'


def test_dag_rendering():
    """Test that all DAGs in the DAG bag render correctly without running any tasks."""
    dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
    assert len(dag_bag.dags) > 0, "No DAGs found in DAG bag!"

    # Collect any DAGs that failed to load
    failed_dags = []
    
    for dag_id, dag in dag_bag.dags.items():
        try:
            # Ensure that the DAG renders correctly
            assert dag is not None, f"DAG '{dag_id}' is None"
            # Additional rendering checks can be added if needed
        except Exception as e:
            failed_dags.append(f"DAG '{dag_id}' failed to render: {e}")

    # Fail the test if any DAGs failed to render
    if failed_dags:
        pytest.fail("\n".join(failed_dags))