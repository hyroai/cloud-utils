import base64
import logging
import os
from typing import Dict, List, Optional, Text

import gamla
import toolz
from kubernetes import client
from kubernetes.client import rest
from toolz import curried

from cloud_utils.k8s import configure


def _set_dry_run(options: Dict, dry_run: bool):
    if dry_run:
        options["dry_run"] = "All"


def _get_cronjob_name(pod_name: Text) -> Text:
    return f"{pod_name}-cronjob"


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
            name=_get_cronjob_name(pod_name), labels={"repository": repo_name},
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
    options = {"namespace": "default", "body": cron_job, "pretty": "true"}
    _set_dry_run(options, dry_run)
    try:
        api_response = client.BatchV1beta1Api().patch_namespaced_cron_job(
            **toolz.assoc(options, "name", _get_cronjob_name(pod_name))
        )
    except rest.ApiException:
        logging.info(f"CronJob {options.get('name')} doesn't exist, creating...")
        api_response = client.BatchV1beta1Api().create_namespaced_cron_job(**options)
    logging.info(f"CronJob updated: {api_response}")

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
        gamla.assoc_in(
            keys=["containers", 0, "env"],
            value=toolz.pipe(
                env_variables,
                gamla.map(
                    lambda env_var: toolz.update_in(
                        env_var, ["value"], lambda value: os.getenv(value, value),
                    ),
                ),
                tuple,
            ),
        ),
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


@toolz.curry
def _delete_cron_job(api_instance: client.BatchV1beta1Api, dry_run: bool, name: Text):
    options = {"name": name, "namespace": "default"}
    _set_dry_run(options, dry_run)
    api_instance.delete_namespaced_cron_job(**options)


def delete_old_cron_jobs(repo_name: Text, new_jobs, dry_run: bool = False):
    api_instance = client.BatchV1beta1Api()
    options = {"namespace": "default", "label_selector": f"repository={repo_name}"}
    api_response = api_instance.list_namespaced_cron_job(**options)
    gamla.pipe(
        api_response.items,
        curried.map(lambda cronjob: cronjob.metadata.name),
        curried.filter(
            lambda cronjob_name: cronjob_name
            not in gamla.pipe(
                new_jobs,
                gamla.map(
                    gamla.compose_left(
                        curried.get_in(["run", "pod_name"]), _get_cronjob_name,
                    ),
                ),
                tuple,
            ),
        ),
        curried.map(_delete_cron_job(api_instance, dry_run)),
        tuple,
    )
    logging.info("Deleted old CronJobs.")


def _get_repo_name_from_image(image: Text):
    return image.split(":")[0].split("/")[-1]


configure.init_kubernetes_client()
