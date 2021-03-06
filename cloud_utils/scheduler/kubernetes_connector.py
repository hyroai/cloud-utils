import base64
import logging
import os
from typing import Dict, List, Optional, Text

import gamla
from kubernetes import client
from kubernetes.client import rest

from cloud_utils.k8s import configure


def _set_dry_run(options: Dict, dry_run: bool):
    if dry_run:
        options["dry_run"] = "All"


_cronjob_name = gamla.wrap_str("{}-cronjob")


def _create_secret(secret: Dict[Text, Text]):
    api_instance = client.CoreV1Api()
    try:
        api_instance.read_namespaced_secret(
            name=secret["secret_name"],
            namespace="default",
        )
    except rest.ApiException as e:
        if e.status != 404:
            raise RuntimeError(f"Unknown error: {e}.")

        api_instance.create_namespaced_secret(
            body=client.V1Secret(
                api_version="v1",
                kind="Secret",
                metadata={"name": secret["secret_name"], "type": "Opaque"},
                data=gamla.valmap(
                    lambda s: base64.b64encode(os.getenv(s, s).encode()).decode(),
                    secret["data"],
                ),
            ),
            namespace="default",
        )
        logging.info(f"Secret '{secret['secret_name']}' created.")


def create_cron_job(
    pod_name: Text,
    image: Text,
    tag: Text,
    env_variables: List[Dict[Text, Text]],
    secrets: List[Dict[Text, Text]],
    command: List[Text],
    args: List[Text],
    schedule: Text,
    dry_run: bool,
    node_selector: Optional[Dict[Text, Text]] = None,
    labels: Optional[Dict[Text, Text]] = None,
) -> Text:
    if secrets:
        for secret in secrets:
            _create_secret(secret)

    cron_job = client.V1beta1CronJob(
        api_version="batch/v1beta1",
        kind="CronJob",
        metadata=client.V1ObjectMeta(name=_cronjob_name(pod_name)),
        spec=client.V1beta1CronJobSpec(
            schedule=schedule,
            concurrency_policy="Forbid",
            successful_jobs_history_limit=1,
            failed_jobs_history_limit=1,
            job_template=client.V1beta1JobTemplateSpec(
                spec=client.V1JobSpec(
                    backoff_limit=1,
                    template=client.V1PodTemplateSpec(
                        metadata=client.V1ObjectMeta(labels=labels),
                        spec=_pod_manifest(
                            pod_name,
                            image,
                            tag,
                            env_variables,
                            secrets,
                            command,
                            args,
                            node_selector,
                        ),
                    ),
                ),
            ),
        ),
    )
    options = {"namespace": "default", "body": cron_job, "pretty": "true"}
    _set_dry_run(options, dry_run)
    try:
        api_response = client.BatchV1beta1Api().patch_namespaced_cron_job(
            **gamla.add_key_value("name", _cronjob_name(pod_name))(options)
        )
    except rest.ApiException:
        logging.info(f"CronJob {options.get('name')} doesn't exist, creating...")
        api_response = client.BatchV1beta1Api().create_namespaced_cron_job(**options)
    logging.info(f"CronJob updated: {api_response}.")

    return pod_name


def _pod_manifest(
    pod_name: Text,
    image: Text,
    tag: Text,
    env_variables: List[Dict[Text, Text]],
    secrets: List[Dict[Text, Text]],
    command: List[Text],
    args: List[Text],
    node_selector: Optional[Dict[Text, Text]],
):
    return gamla.pipe(
        (pod_name, image, tag, node_selector),
        gamla.star(_make_base_pod_spec),
        gamla.assoc_in(
            keys=["containers", 0, "env"],
            value=gamla.pipe(
                env_variables,
                gamla.map(
                    lambda env_var: gamla.update_in(
                        env_var,
                        ["value"],
                        lambda value: os.getenv(value, value),
                    ),
                ),
                tuple,
            ),
        ),
        gamla.compose_left(
            gamla.juxt(*map(_add_volume_from_secret, secrets)),
            gamla.merge,
        )
        if secrets
        else gamla.identity,
        gamla.assoc_in(keys=["containers", 0, "command"], value=command)
        if command
        else gamla.identity,
        gamla.assoc_in(keys=["containers", 0, "args"], value=args)
        if args
        else gamla.identity,
    )


def _make_base_pod_spec(
    pod_name: Text,
    image: Text,
    tag: Text,
    node_selector: Optional[Dict[Text, Text]],
):
    return {
        "containers": [{"image": f"{image}:{tag}", "name": f"{pod_name}-container"}],
        "imagePullSecrets": [
            {"name": f"{_repo_name_from_image(image)}-gitlab-creds"},
        ],
        "restartPolicy": "Never",
        "nodeSelector": node_selector or {"role": "jobs"},
    }


def _add_volume_from_secret(secret: Dict[Text, Text]):
    return gamla.compose_left(
        gamla.assoc_in(
            keys=["containers", 0, "volumeMounts"],
            value=[
                {
                    "name": secret["volume_name"],
                    "mountPath": secret["mount_path"],
                    "readOnly": True,
                },
            ],
        ),
        gamla.assoc_in(
            keys=["volumes"],
            value=[
                {
                    "name": secret["volume_name"],
                    "secret": {"secretName": secret["secret_name"]},
                },
            ],
        ),
    )


def _repo_name_from_image(image: Text):
    return image.split(":")[0].split("/")[-1]


configure.init_kubernetes_client()
