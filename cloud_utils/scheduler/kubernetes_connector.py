import base64
import logging
import os
import time
from typing import Any, Dict, List, Optional, Text

import gamla
from kubernetes import client
from kubernetes.client import rest

from cloud_utils.k8s import configure


def _set_dry_run(options: Dict, dry_run: bool):
    if dry_run:
        options["dry_run"] = "All"


_cronjob_name = gamla.wrap_str("{}-cronjob")
_job_name = gamla.wrap_str("{}-job")


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


def _delete_job(name, options):
    """Deletes job by name.
    Raises ApiException exception if no there is no job with given name"""
    client.BatchV1Api().delete_namespaced_job(
        **gamla.pipe(
            options,
            gamla.add_key_value("name", name),
            gamla.add_key_value(
                "body",
                client.V1DeleteOptions(propagation_policy="Foreground"),
            ),
        )
    )


def _wait_for_job_deletion(name, options):
    """Waits until job is deleted
    Raises ApiException exception when job with given name no longer exist"""
    for i in range(12):
        logging.debug("Waiting for old job to terminate...")
        time.sleep(5)
        client.BatchV1Api().read_namespaced_job(
            **gamla.add_key_value("name", name)(options)
        )


@gamla.curry
def create_job(
    pod_name: Text,
    dry_run: bool,
    job_spec: client.V1JobSpec,
) -> Text:
    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=_job_name(pod_name)),
        spec=job_spec,
    )
    options = {"namespace": "default", "pretty": "true"}
    _set_dry_run(options, dry_run)
    try:
        _delete_job(_job_name(pod_name), options)
        _wait_for_job_deletion(_job_name(pod_name), options)
        logging.error(f"Unable to delete job '{_job_name(pod_name)}'")
    except rest.ApiException:
        logging.debug("Old job deleted")
    finally:
        api_response = client.BatchV1Api().create_namespaced_job(
            **gamla.add_key_value("body", job)(options)
        )
    logging.info(f"Job created: {api_response}.")

    return pod_name


@gamla.curry
def create_cron_job(
    pod_name: Text,
    schedule: Text,
    dry_run: bool,
    job_spec: client.V1JobSpec,
) -> Text:
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
                spec=job_spec,
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


@gamla.curry
def _make_pod_manifest(
    env_variables: List[Dict[Text, Text]],
    secrets: List[Dict[Text, Text]],
    command: List[Text],
    args: List[Text],
    extra_arg: Text,
    base_pod_spec: Dict[Text, Any],
) -> client.V1PodSpec:
    if secrets:
        for secret in secrets:
            _create_secret(secret)
    if extra_arg:
        command[-1] += f" {extra_arg}"
    return gamla.pipe(
        base_pod_spec,
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


@gamla.curry
def _make_job_spec(
    labels: Dict[Text, Text],
    pod_manifest: client.V1PodSpec,
) -> client.V1JobSpec:
    return client.V1JobSpec(
        ttl_seconds_after_finished=60,
        backoff_limit=1,
        template=client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels=labels),
            spec=pod_manifest,
        ),
    )


def _make_base_pod_spec(
    pod_name: Text,
    image: Text,
    tag: Text,
    node_selector: Optional[Text],
) -> Dict[Text, Any]:
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


def make_job_spec(
    run: Dict[Text, Text],
    tag: Text,
    extra_arg: Optional[Text],
) -> client.V1JobSpec:
    return gamla.pipe(
        _make_base_pod_spec(
            run["pod_name"],
            run["image"],
            tag,
            run.get("node_selector"),
        ),
        _make_pod_manifest(
            run.get("env_variables"),
            run.get("secrets"),
            run.get("command"),
            run.get("args"),
            extra_arg,
        ),
        _make_job_spec(run.get("labels")),
    )


configure.init_kubernetes_client()
