import json
import pathlib

from cloud_utils.scheduler import deploy_cron_jobs


def test_scheduler():
    deploy_cron_jobs.deploy_schedule(
        "test-tag",
        True,
        json.load((pathlib.Path(__file__).parent / "test_schedule.json").open()),
    )
