import base64
import logging
import os
from pathlib import Path
from typing import Dict, List, Text

import gamla
import toolz
from kubernetes import client, config
from kubernetes.client import rest


def _get_kubernetes_client() -> client.CoreV1Api:
    kube_config_file = Path("~/.kube/config").expanduser()
    if not kube_config_file.exists():
        config_decoded = base64.b64decode(os.environ["KUBE_CONFIG"])
        Path("~/.kube").expanduser().mkdir(exist_ok=True)
        kube_config_file.touch(exist_ok=True)
        kube_config_file.write_bytes(config_decoded)
    config.load_kube_config()
    return client.CoreV1Api()


def _create_secret(api_instance: client.CoreV1Api, secret: Dict[Text, Text]):
    try:
        api_instance.read_namespaced_secret(
            name=secret["secret_name"], namespace="default"
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
                    lambda s: base64.b64encode(s.encode()).decode(), secret["data"]
                ),
            ),
            namespace="default",
        )
        logging.info(f"Secret '{secret['secret_name']}' created.")


@gamla.curry
def _create_cron_job(
        api_instance: client.CoreV1Api,
        pod_name: Text,
        image: Text,
        env_variables: List[Dict[Text, Text]],
        secrets: List[Dict[Text, Text]],
        command: List[Text],
        args: List[Text],
        schedule: Text,
        dry_run: bool,
) -> Text:
    if secrets:
        for secret in secrets:
            _create_secret(api_instance, secret)

    cron_job = client.V1beta1CronJob(
        api_version="batch/v1beta1",
        kind="CronJob",
        metadata=client.V1ObjectMeta(
            name=f'{pod_name}-cronjob'
        ), spec=client.V1beta1CronJobSpec(
            schedule=schedule,
            job_template=client.V1beta1JobTemplateSpec(
                spec=client.V1JobSpec(
                    template=client.V1PodTemplateSpec(
                        spec=_get_pod_manifest(
                            pod_name, image, env_variables, secrets, command, args
                        )
                    )
                )
            ),
        )
    )
    try:
        api_response = client.BatchV1beta1Api().create_namespaced_cron_job(
            namespace="default",
            body=cron_job,
            pretty='true',
            dry_run='All' if dry_run else None
        )
        logging.info(f"CronJob created: {api_response}")
    except rest.ApiException as e:
        logging.error(f"Error creating CronJob: {e}")
        raise
    return pod_name


def _get_pod_manifest(
        pod_name: Text,
        image: Text,
        env_variables: List[Dict[Text, Text]],
        secrets: List[Dict[Text, Text]],
        command: List[Text],
        args: List[Text],
):
    return toolz.pipe(
        (pod_name, image),
        gamla.star(_make_base_pod_spec),
        gamla.assoc_in(keys=["containers", 0, "env"], value=env_variables),
        toolz.compose_left(
            toolz.juxt(*map(_add_volume_from_secret, secrets)), toolz.merge
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


def _make_base_pod_spec(pod_name: Text, image: Text):
    return {
        "containers": [{"image": image, "name": f"{pod_name}-container"}],
        "imagePullSecrets": [
            {"name": f"{_get_repo_name_from_image(image)}-gitlab-creds"}
        ],
        "restartPolicy": "Never",
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
                }
            ],
        ),
        gamla.assoc_in(
            keys=["volumes"],
            value=[
                {
                    "name": secret["volume_name"],
                    "secret": {"secretName": secret["secret_name"]},
                }
            ],
        ),
    )


make_deploy_cron_job = gamla.compose_left(
    _get_kubernetes_client,
    toolz.juxt(_create_cron_job),
    gamla.star(gamla.compose_left),
)


def _get_repo_name_from_image(image: Text):
    return image.split(":")[0].split("/")[-1]
