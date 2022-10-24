import json
import pathlib

import pytest

from cloud_utils.scheduler import deploy_cron_jobs, run_jobs


@pytest.mark.skip("ENG-2899")
def test_scheduler():
    deploy_cron_jobs.deploy_schedule(
        "test-tag",
        True,
        json.load((pathlib.Path(__file__).parent / "test_schedule.json").open()),
    )


@pytest.mark.skip("ENG-2899")
def test_run_jobs():
    run_jobs.deploy_jobs(
        "test-tag",
        True,
        json.load((pathlib.Path(__file__).parent / "test_schedule.json").open()),
        "extra-arg",
        0,
    )


@pytest.mark.skip("ENG-2899")
def test_empty_scheduler():
    deploy_cron_jobs.deploy_schedule(
        "test-tag",
        True,
        json.load((pathlib.Path(__file__).parent / "test_schedule_empty.json").open()),
    )
