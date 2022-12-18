import argparse
import json
import sys
from typing import Dict, Iterable, Optional, Sequence

import gamla

from cloud_utils.scheduler import kubernetes_connector


def deploy_schedule(tag: str, dry_run: bool, job_configs: Iterable[Dict]):
    job_configs = tuple(job_configs)  # Defend from generators.
    for config in job_configs:
        run = config["run"]
        gamla.pipe(
            kubernetes_connector.make_job_spec(run, tag, None),
            kubernetes_connector.create_cron_job(
                run.get("namespace", "default"),
                run["pod_name"],
                config["schedule"],
                dry_run,
            ),
        )


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

    args = parser.parse_args(argv)

    deploy_schedule(args.tag, False, json.load(open(args.schedule)))
    return 0


if __name__ == "__main__":
    exit(main())
