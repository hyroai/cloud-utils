import argparse
import json
import sys
from typing import Dict, Iterable, Optional, Sequence

import gamla

from cloud_utils.config import logging_config  # noqa: F401
from cloud_utils.scheduler import kubernetes_connector


def deploy_jobs(
    tag: str,
    dry_run: bool,
    job_configs: Iterable[Dict],
    extra_arg: str,
    wait_minutes_for_completion: int,
):
    job_configs = tuple(job_configs)  # Defend from generators.
    for config in job_configs:
        run = config["run"]
        gamla.pipe(
            kubernetes_connector.make_job_spec(run, tag, extra_arg),
            kubernetes_connector.create_job(
                run.get("namespace", "default"),
                run["pod_name"],
                dry_run,
                wait_minutes_for_completion,
            ),
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(
        description="Creates k8s jobs from jobs.json file.",
    )
    parser.add_argument(
        "--jobs",
        type=str,
        help="Path to jobs file, defaults to `jobs.json`.",
    )
    parser.add_argument(
        "--tag",
        type=str,
        help="The image tag, defaults to latest.",
        default="latest",
    )
    parser.add_argument(
        "--extra_argument",
        type=str,
        help="Additional single argument to the pod command",
    )

    parser.add_argument(
        "--wait_minutes_for_completion",
        type=int,
        default=0,
        help="Wait given number of minutes for the job to complete before raising an exception. 0 means don't wait",
    )

    args = parser.parse_args(argv)

    deploy_jobs(
        args.tag,
        False,
        json.load(open(args.jobs)),
        args.extra_argument,
        args.wait_minutes_for_completion,
    )
    return 0


if __name__ == "__main__":
    exit(main())
