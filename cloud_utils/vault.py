from typing import Callable, Optional

from hvac import Client
from hvac.api.auth_methods import Kubernetes
from hvac.exceptions import InvalidPath

_MOUNT_POINT = "secret"


class InvalidVaultPath(Exception):
    pass


def _build_read_secret(client: Client) -> Callable:
    def read_secret(path: str, version: Optional[str]) -> dict[str, str]:
        try:
            secret = client.secrets.kv.v2.read_secret_version(
                path=path, version=version
            )
            return secret["data"]["data"]
        except InvalidPath:
            raise InvalidVaultPath(f"Invalid vault path: {path}")

    return read_secret


def _build_write_or_update_secret(client: Client, read_secret: Callable) -> Callable:
    def write_or_update_secret(path: str, new_keys: dict[str, str]):
        try:
            existing_keys = read_secret(path, None)
        except InvalidVaultPath:
            existing_keys = {}
        existing_keys.update(new_keys)
        client.secrets.kv.v2.create_or_update_secret(
            path=path,
            secret=existing_keys,
            mount_point=_MOUNT_POINT,
        )

    return write_or_update_secret


def make_vault(host: str, role: Optional[str], token: Optional[str]):
    if not token and not role:
        raise Exception("Must specify either token or role")

    if token:
        client = Client(url=host, token=token)
    else:
        client = Client(url=host)
        Kubernetes(client.adapter).login(
            role=role,
            jwt=open("/var/run/secrets/kubernetes.io/serviceaccount/token").read(),
        )

    read_secret = _build_read_secret(client)
    write_or_update_secret = _build_write_or_update_secret(client, read_secret)
    return read_secret, write_or_update_secret
