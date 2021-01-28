import argparse
import asyncio
import json
import sys
from typing import Dict, Iterable, Optional, Sequence, Text

import gamla

from cloud_utils.scheduler import kubernetes_connector


@gamla.curry
async def deploy_schedule(tag: Text, dry_run: bool, job_configs: Iterable[Dict]):
    job_configs = tuple(job_configs)  # Defend from generators.
    for config in job_configs:
        kubernetes_connector.create_cron_job(
            **{
                **config["run"],
                "schedule": config["schedule"],
                "tag": tag,
                "dry_run": dry_run,
            }
        )


def _get_filter_stage(args):
    if args.job:
        return gamla.filter(
            gamla.compose_left(
                gamla.get_in(["run", "pod_name"]),
                gamla.equals(args.job),
            ),
        )
    return gamla.identity


def main(argv: Optional[Sequence[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(
        description="Creates k8s CronJobs from schedule.json file.",
    )
    parser.add_argument(
        "--schedule",
        type=str,
        help="Path to schedule file, defaults to `schedule.json`.",
    )
    parser.add_argument(
        "--tag",
        type=str,
        help="The image tag, defaults to latest.",
        default="latest",
    )
    parser.add_argument(
        "--job",
        type=str,
        help="The entries in the configuration have pod names which serve as the job key (they are assumed to be unique).",
        default=None,
    )
    args = parser.parse_args(argv)

    gamla.pipe(
        args.schedule,
        open,
        json.load,
        _get_filter_stage(args),
        gamla.star(deploy_schedule(args.tag, False)),
        asyncio.get_event_loop().run_until_complete,
    )
    return 0


if __name__ == "__main__":
    exit(main())
