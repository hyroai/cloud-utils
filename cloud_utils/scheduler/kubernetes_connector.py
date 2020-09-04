import base64
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Text
import os

import gamla
import toolz
from kubernetes import client
from kubernetes.client import rest
from cloud_utils.k8s import configure


def _create_secret(secret: Dict[Text, Text]):
    api_instance = client.CoreV1Api()
    try:
        api_instance.read_namespaced_secret(
            name=secret["secret_name"], namespace="default",
        )
    except rest.ApiException as e:
        if e.status != 404:
            raise RuntimeError(f"Unknown error: {e}")

        api_instance.create_namespaced_secret(
            body=client.V1Secret(
                api_version="v1",
                kind="Secret",
                metadata={"name": secret["secret_name"], "type": "Opaque"},
                data=toolz.valmap(
                    lambda s: base64.b64encode(os.getenv(s, s).encode()).decode(), secret["data"],
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
    repo_name: Text,
    dry_run: bool,
    node_selector: Optional[Dict[Text, Text]] = None,
) -> Text:
    if secrets:
        for secret in secrets:
            _create_secret(secret)

    cron_job = client.V1beta1CronJob(
        api_version="batch/v1beta1",
        kind="CronJob",
        metadata=client.V1ObjectMeta(
            name=f"{pod_name}-cronjob", labels={"repository": repo_name},
        ),
        spec=client.V1beta1CronJobSpec(
            schedule=schedule,
            concurrency_policy="Forbid",
            successful_jobs_history_limit=1,
            failed_jobs_history_limit=1,
            job_template=client.V1beta1JobTemplateSpec(
                spec=client.V1JobSpec(
                    backoff_limit=1,
                    template=client.V1PodTemplateSpec(
                        spec=_get_pod_manifest(
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
    try:
        options = {"namespace": "default", "body": cron_job, "pretty": "true"}
        if dry_run:
            options["dry_run"] = "All"
        api_response = client.BatchV1beta1Api().create_namespaced_cron_job(**options)
        logging.info(f"CronJob created: {api_response}")
    except rest.ApiException as e:
        logging.error(f"Error creating CronJob: {e}")
        raise
    return pod_name


def _get_pod_manifest(
    pod_name: Text,
    image: Text,
    tag: Text,
    env_variables: List[Dict[Text, Text]],
    secrets: List[Dict[Text, Text]],
    command: List[Text],
    args: List[Text],
    node_selector: Optional[Dict[Text, Text]],
):
    return toolz.pipe(
        (pod_name, image, tag, node_selector),
        gamla.star(_make_base_pod_spec),
        gamla.assoc_in(keys=["containers", 0, "env"], value=env_variables),
        toolz.compose_left(
            toolz.juxt(*map(_add_volume_from_secret, secrets)), toolz.merge,
        )
        if secrets
        else toolz.identity,
        gamla.assoc_in(keys=["containers", 0, "command"], value=command)
        if command
        else toolz.identity,
        gamla.assoc_in(keys=["containers", 0, "args"], value=args)
        if args
        else toolz.identity,
    )


def _make_base_pod_spec(
    pod_name: Text, image: Text, tag: Text, node_selector: Optional[Dict[Text, Text]],
):
    return {
        "containers": [{"image": f"{image}:{tag}", "name": f"{pod_name}-container"}],
        "imagePullSecrets": [
            {"name": f"{_get_repo_name_from_image(image)}-gitlab-creds"},
        ],
        "restartPolicy": "Never",
        "nodeSelector": node_selector or {"role": "jobs"},
    }


def _add_volume_from_secret(secret: Dict[Text, Text]):
    return toolz.compose_left(
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


def delete_old_cron_jobs(repo_name: Text, dry_run: bool = False):
    api_instance = client.BatchV1beta1Api()
    options = {"namespace": "default", "label_selector": f"repository={repo_name}"}
    if dry_run:
        options["dry_run"] = "All"
    api_instance.delete_collection_namespaced_cron_job(**options)
    logging.info("Deleting CronJobs. Waiting 2 min for deletion to complete...")
    # TODO(Erez): Find an event driven way to wait for this
    time.sleep(2 * 60)


def _get_repo_name_from_image(image: Text):
    return image.split(":")[0].split("/")[-1]


configure.init_kubernetes_client()
