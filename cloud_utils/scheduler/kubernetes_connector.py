import logging
import os
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Text, Tuple

import gamla
from kubernetes import client
from kubernetes.client import rest

from cloud_utils.k8s import configure, utils


def _set_dry_run(options: Dict, dry_run: bool):
    if dry_run:
        options["dry_run"] = "All"


_cronjob_name = gamla.wrap_str("{}-cronjob")


def _wait_for_job_completion(
    wait_minutes_for_completion: int,
    name: str,
    options: Dict,
):
    """Waits until job is completed
    Raises ApiException exception when job with given name no longer exist"""
    for i in range(wait_minutes_for_completion):
        logging.info("Waiting for job to complete...")
        job = client.BatchV1Api().read_namespaced_job_status(
            **gamla.add_key_value("name", name)(options)
        )

        if job.status.succeeded:
            return
        if (
            job.status.failed and job.status.failed > 1
        ):  # We have one retry, so we want to wait for the 2nd failure.
            job_pods = client.CoreV1Api().list_namespaced_pod(
                **gamla.add_key_value(
                    "label_selector",
                    f"job-name={job.metadata.name}",
                )(options)
            )
            logs = client.CoreV1Api().read_namespaced_pod_log(
                job_pods.items[0].metadata.name,
                options.get("namespace"),
                tail_lines=200,
            )
            logging.error(logs)
            raise Exception(f"Job failed {logs}")
        time.sleep(60)
    raise Exception(f"Job wasn't completed within {wait_minutes_for_completion} min")


@gamla.curry
def create_job(
    namespace: str,
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
    options = {"namespace": namespace, "pretty": "true"}
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
    namespace: str,
    pod_name: str,
    schedule: str,
    dry_run: bool,
    job_spec: client.V1JobSpec,
) -> Text:
    cron_job = client.V1CronJob(
        api_version="batch/v1",
        kind="CronJob",
        metadata=client.V1ObjectMeta(name=_cronjob_name(pod_name)),
        spec=client.V1CronJobSpec(
            schedule=schedule,
            concurrency_policy="Forbid",
            successful_jobs_history_limit=1,
            failed_jobs_history_limit=1,
            job_template=client.V1JobTemplateSpec(
                spec=job_spec,
            ),
        ),
    )
    options = {"namespace": namespace, "body": cron_job, "pretty": "true"}
    _set_dry_run(options, dry_run)
    try:
        api_response = client.BatchV1Api().patch_namespaced_cron_job(
            **gamla.add_key_value("name", _cronjob_name(pod_name))(options)
        )
    except rest.ApiException:
        logging.info(f"CronJob {options.get('name')} doesn't exist, creating...")
        api_response = client.BatchV1Api().create_namespaced_cron_job(**options)
    logging.info(f"CronJob updated: {api_response}.")

    return pod_name


@gamla.curry
def _make_pod_manifest(
    env_variables: List[Dict[Text, Text]],
    secrets: List[Dict[Text, List[str]]],
    secret_volumes: List[Dict[Text, Text]],
    env_from_secrets: List[str],  # Deprecated
    secret_provider_class: str,  # Deprecated
    secrets_name_prefix: str,  # Deprecated
    resources: Dict[str, Dict[str, str]],
    command: List[Text],
    args: List[Text],
    extra_arg: Optional[str],
    base_pod_spec: Dict[Text, Any],
) -> client.V1PodSpec:
    """Creates a V1PodSpec object with given parameters
    env_variables - list of { name, value } dicts to set as environment vars.
    secrets - A list of Kubernetes secrets and their keys to set as environment vars.
    secret_volumes - A list of Kubernetes secrets to mount as volumes.
    env_from_secrets - list of strings corresponding with Vault keys available to the pod.
    secret_provider_class - a name of a SecretProviderClass to provide the env_from_secrets.
    resources - Kubernetes resources, Dict of { requests, limits }.
    command - Kubernetes command, list of command to run on start.
    args - Kubernetes args, list of args to run on start.
    extra_arg - Additional command on top of the commands list. Used as a cmd parameter and not via json file.
    base_pod_spec - see _make_base_pod_spec function.
    """
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
                # TODO(erez): Remove once finish migrating to vault operator.
                gamla.concat_with(
                    gamla.map(
                        lambda env_var: {
                            "name": env_var,
                            "valueFrom": {
                                "secretKeyRef": {
                                    "name": f"{secrets_name_prefix}{utils.to_kebab_case(env_var)}-vault-secret",
                                    "key": env_var,
                                },
                            },
                        },
                    )(env_from_secrets),
                ),
                gamla.concat_with(
                    gamla.mapcat(_env_vars_from_secret)(secrets),
                ),
                tuple,
            ),
        ),
        gamla.when(
            gamla.just(gamla.nonempty(secret_volumes)),
            gamla.compose_left(
                gamla.juxt(*map(_add_volume_from_secret, secret_volumes)),
                gamla.merge,
            ),
        ),
        # TODO(erez): Remove once finish migrating to vault operator.
        gamla.when(
            gamla.just(gamla.nonempty(secret_provider_class)),
            _add_secret_provider_volume(secret_provider_class),
        ),
        gamla.assoc_in(keys=["containers", 0, "resources"], value=resources),
        gamla.assoc_in(keys=["containers", 0, "command"], value=command),
        gamla.assoc_in(keys=["containers", 0, "args"], value=args),
    )


@gamla.curry
def _make_job_spec(
    labels: Dict[Text, Text],
    annotations: Dict[Text, Text],
    pod_manifest: client.V1PodSpec,
) -> client.V1JobSpec:
    return client.V1JobSpec(
        ttl_seconds_after_finished=60 * 60 * 24,  # Keep job for 24 hours
        active_deadline_seconds=60 * 60 * 4,  # Limit job runtime to 3 hours
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
    service_account_name: Optional[str],
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


def _add_volume_and_mount(volume: Dict[str, Any], mount: Dict[str, Any]):
    return gamla.compose_left(
        gamla.update_in(
            keys=["containers", 0, "volumeMounts"],
            func=gamla.compose_left(
                gamla.concat_with(
                    [
                        mount,
                    ],
                ),
                tuple,
            ),
            default=[],
        ),
        gamla.update_in(
            keys=["volumes"],
            func=gamla.compose_left(
                gamla.concat_with(
                    [
                        volume,
                    ],
                ),
                tuple,
            ),
            default=[],
        ),
    )


def _add_volume_from_secret(secret: Dict[Text, Text]):
    return _add_volume_and_mount(
        {
            "name": secret["volume_name"],
            "secret": {"secretName": secret["secret_name"]},
        },
        {
            "name": secret["volume_name"],
            "mountPath": secret["mount_path"],
            "readOnly": True,
        },
    )


def _env_vars_from_secret(secret: Dict[str, List[str]]):
    return tuple(
        gamla.map(
            lambda key: {
                "name": key,
                "valueFrom": {
                    "secretKeyRef": {
                        "name": secret["secret_name"],
                        "key": key,
                    },
                },
            },
        )(secret["keys"]),
    )


def _add_secret_provider_volume(provider_name: str):
    return _add_volume_and_mount(
        {
            "name": "secrets-store-inline",
            "csi": {
                "driver": "secrets-store.csi.k8s.io",
                "readOnly": True,
                "volumeAttributes": {"secretProviderClass": provider_name},
            },
        },
        {
            "name": "secrets-store-inline",
            "mountPath": "/mnt/secrets-store",
            "readOnly": True,
        },
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
            run.get("serviceAccountName"),
        ),
        _make_pod_manifest(
            run.get("env_variables", []),
            run.get("secrets", []),
            run.get("secret_volumes", []),
            run.get("env_from_secrets", []),
            run.get("secret_provider_class", ""),
            run.get("secrets_name_prefix", ""),
            run.get("resources", {}),
            run.get("command", []),
            run.get("args", []),
            extra_arg,
        ),
        _make_job_spec(
            run.get("labels"),
            run.get("annotations"),
        ),
    )


configure.init_kubernetes_client()
