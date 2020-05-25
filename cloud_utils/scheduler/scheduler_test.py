import json
import pathlib

import pytest

from cloud_utils.scheduler import deploy_cron_jobs

# All test coroutines will be treated as marked.
pytestmark = pytest.mark.asyncio


async def test_scheduler():

    await deploy_cron_jobs.deploy_schedule(
        json.load((pathlib.Path(__file__).parent / "test_schedule.json").open()),
        "test-repo-name",
        dry_run=True,
    )
