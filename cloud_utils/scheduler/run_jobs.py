import argparse
import json
import sys
from typing import Dict, Iterable, Optional, Sequence, Text

from cloud_utils.scheduler import kubernetes_connector


def deploy_jobs(tag: Text, dry_run: bool, job_configs: Iterable[Dict]):
    job_configs = tuple(job_configs)  # Defend from generators.
    for config in job_configs:
        kubernetes_connector.create_job(
            **{
                **config["run"],
                "tag": tag,
                "dry_run": dry_run,
            }
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

    args = parser.parse_args(argv)

    deploy_jobs(args.tag, False, json.load(open(args.jobs)))
    return 0


if __name__ == "__main__":
    exit(main())
