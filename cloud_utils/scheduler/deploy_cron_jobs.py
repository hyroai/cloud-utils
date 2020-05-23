import asyncio
import json
from typing import Dict, Iterable

from cloud_utils.scheduler import kubernetes_connector


async def deploy_schedule(job_configs: Iterable[Dict], dry_run: bool = False):
    kubernetes_connector.delete_old_cron_jobs(dry_run=dry_run)
    for config in job_configs:
        kubernetes_connector.create_cron_job(**{
            **config["run"],
            "schedule": config["schedule"],
            "dry_run": dry_run,
        })


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(
        deploy_schedule(json.load(open("./schedule.json")))
    )
