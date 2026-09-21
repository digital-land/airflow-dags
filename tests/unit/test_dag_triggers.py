import json
from pathlib import Path

import pendulum
import pytest
from airflow.models import DagBag

from dags.collection_config import collection_schedule_matches

CONFIG_PATH = Path("dags/config.json")
TRIGGER_DAG_ID = "trigger-title-boundary-monthly"
COLLECTION_DAG_ID = "new-title-boundary-collection"
SCHEDULED_MASTER_DAG_ID = "trigger-collection-dags-scheduled"
MANUAL_MASTER_DAG_ID = "trigger-collection-dags-manual"


@pytest.fixture(scope="module")
def dag_bag():
    """A DagBag built with title-boundary selected.

    dag_triggers reads config.json at import time and only builds the decoupled trigger DAG for a
    collection the environment has actually selected. conftest's test config selects
    ancient-woodland and organisation only, so without this the DAG is simply absent from the bag
    and every assertion here would pass vacuously against a DAG that was never built.

    get_config re-reads the file on each call and DagBag re-executes each DAG module, so rewriting
    the file and building a new bag is enough - no module reload is involved.
    """
    original = CONFIG_PATH.read_text()
    config = json.loads(original)
    config["collections"] = sorted({*config["collections"], "title-boundary"})
    CONFIG_PATH.write_text(json.dumps(config, indent=4))
    try:
        yield DagBag(dag_folder="dags", include_examples=False)
    finally:
        CONFIG_PATH.write_text(original)


def test_title_boundary_trigger_dag_is_built_when_the_collection_is_selected(dag_bag):
    assert dag_bag.import_errors == {}
    assert TRIGGER_DAG_ID in dag_bag.dags


def test_title_boundary_trigger_dag_triggers_the_collection_without_waiting(dag_bag):
    """wait_for_completion=False is the whole point of the decoupling - whatever starts
    title-boundary must not then sit and block on it for hours."""
    trigger = dag_bag.dags[TRIGGER_DAG_ID].get_task("trigger-title-boundary-collection-dag")

    assert trigger.trigger_dag_id == COLLECTION_DAG_ID
    assert trigger.wait_for_completion is False


@pytest.mark.parametrize("master_dag_id", [SCHEDULED_MASTER_DAG_ID, MANUAL_MASTER_DAG_ID])
def test_title_boundary_is_not_triggered_from_either_master_dag(dag_bag, master_dag_id):
    """The manual master DAG is deliberately kept as close an approximation of the scheduled one
    as possible, so title-boundary has to leave both. Dropping it from the scheduled DAG alone
    would still let a manual run block behind it."""
    title_boundary_tasks = [task.task_id for task in dag_bag.dags[master_dag_id].tasks if "title-boundary" in task.task_id]

    assert title_boundary_tasks == []


def test_digital_land_builder_no_longer_waits_for_title_boundary(dag_bag):
    """The point of the ticket. Every collection task is an upstream of the builder, and each is
    triggered with wait_for_completion=True, so while title-boundary was in that loop the whole
    nightly run sat behind a three-hour monthly job once a month - the 2026-09-07 run was
    cancelled at its timeout while the chain waited on it."""
    builder = dag_bag.dags[SCHEDULED_MASTER_DAG_ID].get_task("trigger-digital-land-builder-dag")

    assert [task_id for task_id in builder.upstream_task_ids if "title-boundary" in task_id] == []


def test_trigger_dag_is_scheduled_at_a_time_the_rrule_check_can_match(dag_bag):
    """The schedule and collection_schedule_matches are coupled through the time of day.

    rrule occurrences expand at RRULE_SERIES_START's time, which is 00:00. A DAG scheduled at any
    other time puts data_interval_end after that day's occurrence, so the check steps over it,
    finds the following month's instead, and skips the collection - silently, every month, with
    the pipeline reporting success throughout.

    Deriving the time from the DAG's own cron rather than hardcoding it means that moving the
    schedule fails here, rather than in production a month later.
    """
    dag = dag_bag.dags[TRIGGER_DAG_ID]
    minute, hour = dag.schedule_interval.split()[:2]

    # 8 Feb 2027 is the Monday after the first Sunday, and one of the five months in which the
    # old first-Monday rule diverged from the corrected one
    data_interval_end = pendulum.datetime(2027, 2, 8, int(hour), int(minute), tz="UTC")

    assert collection_schedule_matches("title-boundary", data_interval_end) is True, (
        f"schedule '{dag.schedule_interval}' puts data_interval_end at {int(hour):02d}:{int(minute):02d}, which collection_schedule_matches cannot match - "
        "see the comment on the schedule in dag_triggers.py"
    )
