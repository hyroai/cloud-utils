import base64
import logging
import os
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Text, Tuple

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


def _wait_for_job_completion(
    wait_minutes_for_completion: int,
    name: str,
    options: Dict,
):
    """Waits until job is completed
    Raises ApiException exception when job with given name no longer exist"""
    for i in range(wait_minutes_for_completion):
        logging.debug("Waiting for job to complete...")
        job = client.BatchV1Api().read_namespaced_job_status(
            **gamla.add_key_value("name", name)(options)
        )
        if job.status.succeeded:
            return
        time.sleep(60)
    raise Exception(f"Job wasn't completed within {wait_minutes_for_completion} min")


@gamla.curry
def create_job(
    pod_name: str,
    dry_run: bool,
    wait_minutes_for_completion: int,
    job_spec: client.V1JobSpec,
) -> Text:
    job_name = f"{pod_name}-job-{str(uuid.uuid4())[:6]}"
    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=job_name),
        spec=job_spec,
    )
    options = {"namespace": "default", "pretty": "true"}
    _set_dry_run(options, dry_run)
    api_response = client.BatchV1Api().create_namespaced_job(
        **gamla.add_key_value("body", job)(options)
    )
    logging.info(f"Job created: {api_response}.")
    if wait_minutes_for_completion:
        _wait_for_job_completion(
            wait_minutes_for_completion,
            job_name,
            options,
        )
    return pod_name


@gamla.curry
def create_cron_job(
    pod_name: str,
    schedule: str,
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
    extra_arg: str,
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
    annotations: Dict[Text, Text],
    pod_manifest: client.V1PodSpec,
) -> client.V1JobSpec:
    return client.V1JobSpec(
        ttl_seconds_after_finished=60 * 60 * 24,  # Keep job for 24 hours
        active_deadline_seconds=60 * 60 * 3,  # Limit job runtime to 3 hours
        backoff_limit=1,
        template=client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels=labels, annotations=annotations),
            spec=pod_manifest,
        ),
    )


_DEFAULT_JOB_TOLERATIONS = (
    {
        "key": "kubernetes.azure.com/scalesetpriority",
        "value": "spot",
        "effect": "NoSchedule",
    },
)

_DEFAULT_JOB_ANNOTATIONS = {
    "vault.hashicorp.com/agent-inject": "true",
    "vault.hashicorp.com/agent-inject-secret-env": "secret/data/dev",
    "vault.hashicorp.com/agent-inject-template-env": '{{ with secret "secret/data/dev" }}\n  {{ range $k, $v := .Data.data }}\n    {{ $k }}={{ $v }}\n  {{ end }}\n{{ end }}\n',
    "vault.hashicorp.com/role": "dev-app",
}


_make_tolerations: Callable[
    [Tuple[Dict[str, str], ...]],
    Tuple[client.V1Toleration, ...],
] = gamla.compose_left(
    gamla.map(
        lambda toleration: client.V1Toleration(
            key=toleration.get("key"),
            value=toleration.get("value"),
            effect=toleration.get("effect"),
        ),
    ),
    tuple,
)


def _make_base_pod_spec(
    pod_name: str,
    image: str,
    tag: str,
    node_selector: Dict[str, str],
    tolerations: Tuple[Dict, ...],
    service_account_name: str,
) -> Dict[Text, Any]:
    return {
        "containers": [{"image": f"{image}:{tag}", "name": f"{pod_name}-container"}],
        "imagePullSecrets": [
            {"name": f"{_repo_name_from_image(image)}-gitlab-creds"},
        ],
        "restartPolicy": "Never",
        "nodeSelector": node_selector,
        "tolerations": _make_tolerations(tolerations),
        "serviceAccountName": service_account_name,
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


def _repo_name_from_image(image: str):
    return image.split(":")[0].split("/")[-1]


def make_job_spec(
    run: Dict[str, Any],
    tag: str,
    extra_arg: Optional[Text],
) -> client.V1JobSpec:
    return gamla.pipe(
        _make_base_pod_spec(
            run["pod_name"],
            run["image"],
            tag,
            run.get("node_selector", {"role": "jobs"}),
            run.get("tolerations", _DEFAULT_JOB_TOLERATIONS),
            run.get("serviceAccountName", "dev-app"),
        ),
        _make_pod_manifest(
            run.get("env_variables"),
            run.get("secrets"),
            run.get("command"),
            run.get("args"),
            extra_arg,
        ),
        _make_job_spec(
            run.get("labels"),
            run.get("annotations", _DEFAULT_JOB_ANNOTATIONS),
        ),
    )


configure.init_kubernetes_client()
