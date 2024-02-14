from typing import Callable, Optional

import hvac
from hvac import exceptions
from hvac.api.auth_methods import Kubernetes

_MOUNT_POINT = "secret"


def _build_read_all_secrets(client: hvac.Client):
    def read_all_secrets(path: str, version: Optional[str]) -> dict[str, str]:
        read_secrets = client.secrets.kv.v2.read_secret_version(
            path=path, version=version
        )
        return read_secrets["data"]["data"]

    return read_all_secrets


def _build_write_or_update_secrets(client: hvac.Client, read_all_secrets: Callable):
    def write_or_update_secrets(path: str, secrets: dict[str, str]):
        try:
            secrets_data = read_all_secrets(path, None)
        except hvac.exceptions.InvalidPath:
            secrets_data = {}
        secrets_data.update(secrets)
        client.secrets.kv.v2.create_or_update_secret(
            path=path,
            secret=secrets_data,
            mount_point=_MOUNT_POINT,
        )

    return write_or_update_secrets


def make_vault(host: str, token: Optional[str], role: Optional[str]):
    if not token and not role:
        raise Exception("Must specify either token or role")

    if token:
        client = hvac.Client(url=host, token=token)
    else:
        client = hvac.Client(url=host)
        Kubernetes(client.adapter).login(
            role=role,
            jwt=open("/var/run/secrets/kubernetes.io/serviceaccount/token").read(),
        )

    read_all_secrets = _build_read_all_secrets(client)
    write_or_update_secrets = _build_write_or_update_secrets(client, read_all_secrets)
    return read_all_secrets, write_or_update_secrets
