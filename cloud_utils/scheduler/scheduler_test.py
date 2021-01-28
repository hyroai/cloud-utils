import json
import pathlib

import pytest

from cloud_utils.scheduler import deploy_cron_jobs

pytestmark = pytest.mark.asyncio


async def test_scheduler():
    await deploy_cron_jobs.deploy_schedule(
        "test-repo-name",
        "test-tag",
        True,
        json.load((pathlib.Path(__file__).parent / "test_schedule.json").open()),
    )
