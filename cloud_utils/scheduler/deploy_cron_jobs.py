import argparse
import asyncio
import json
import sys
from typing import Dict, Iterable, Optional, Sequence

from cloud_utils.scheduler import kubernetes_connector


async def deploy_schedule(
    job_configs: Iterable[Dict], repo_name, dry_run: bool = False
):
    kubernetes_connector.delete_old_cron_jobs(repo_name, dry_run=dry_run)
    for config in job_configs:
        kubernetes_connector.create_cron_job(
            **{
                **config["run"],
                "schedule": config["schedule"],
                "repo_name": repo_name,
                "dry_run": dry_run,
            }
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(
        description="Creates k8s CronJobs from schedule.json file"
    )
    parser.add_argument(
        "repo",
        type=str,
        help="Repository name or another unique identifier for the jobs.",
    )
    parser.add_argument(
        "--schedule",
        type=str,
        help="Path to schedule file, defaults to `schedule.json`.",
        default="schedule.json",
    )
    args = parser.parse_args(argv)
    asyncio.get_event_loop().run_until_complete(
        deploy_schedule(json.load(open(args.schedule)), args.repo)
    )
    return 0


if __name__ == "__main__":
    exit(main())
