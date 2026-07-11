from typing import Callable, Optional

import httpx

from cloud_utils import vault_shared

_TIMEOUT = 5.0


async def _k8s_login(host: str, role: str) -> str:
    jwt = open(vault_shared.K8S_JWT_PATH).read()
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        response = await client.post(
            f"{host}/v1/auth/kubernetes/login", json={"role": role, "jwt": jwt}
        )
    response.raise_for_status()
    return response.json()["auth"]["client_token"]


def _build_read_secret(host: str, token: str) -> Callable:
    async def read_secret(path: str, version: Optional[str]) -> dict[str, str]:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            response = await client.get(
                f"{host}/v1/{vault_shared.MOUNT_POINT}/data/{path}",
                params={} if version is None else {"version": version},
                headers={"X-Vault-Token": token},
            )
        if response.status_code == 404:
            raise vault_shared.InvalidVaultPathError(f"Invalid vault path: {path}")
        response.raise_for_status()
        return response.json()["data"]["data"]

    return read_secret


def _build_write_or_update_secret(
    host: str, token: str, read_secret: Callable
) -> Callable:
    async def write_or_update_secret(path: str, new_keys: dict[str, str]) -> None:
        try:
            existing_keys = await read_secret(path, None)
        except vault_shared.InvalidVaultPathError:
            existing_keys = {}
        existing_keys.update(new_keys)
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            response = await client.post(
                f"{host}/v1/{vault_shared.MOUNT_POINT}/data/{path}",
                json={"data": existing_keys},
                headers={"X-Vault-Token": token},
            )
        response.raise_for_status()

    return write_or_update_secret


async def make_vault_async(
    host: str, role: Optional[str], token: Optional[str]
) -> tuple[Callable, Callable]:
    if not token and not role:
        raise Exception("Must specify either token or role")
    if not token:
        token = await _k8s_login(host, role)
    read_secret = _build_read_secret(host, token)
    return read_secret, _build_write_or_update_secret(host, token, read_secret)