from typing import Any, Callable, Coroutine, Dict, Optional

import gamla
from cache import AsyncTTL

base_metadata_url = "http://169.254.169.254/metadata"


class KeyDoesNotExist(Exception):
    pass


async def _identity_token():
    return (
        (
            await gamla.get_async_with_headers(
                {"Metadata": "true"},
                30,
                f"{base_metadata_url}/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fmanagement.azure.com%2F",
            )
        )
        .json()
        .get("access_token")
    )


async def _instance_metadata():
    return (
        await gamla.get_async_with_headers(
            {"Metadata": "true"},
            30,
            f"{base_metadata_url}/instance?api-version=2019-08-15",
        )
    ).json()


def _make_pod_identity_token(
    role: str,
    base_vault_url: str,
) -> Callable[[], Coroutine[Any, Any, str]]:
    @AsyncTTL(time_to_live=60 * 60 * 23, maxsize=1)  # Vault token is valid for 24 hour.
    async def _pod_identity_token() -> str:
        jwt = await _identity_token()
        return await gamla.pipe(
            await _instance_metadata(),
            gamla.get_in(["compute"]),
            lambda compute: {
                "role": role,
                "jwt": jwt,
                "subscription_id": gamla.get_in(["subscriptionId"])(compute),
                "resource_group_name": gamla.get_in(["resourceGroupName"])(compute),
                "vm_name": gamla.get_in(["name"])(compute),
                "vmss_name": gamla.get_in(["vmScaleSetName"])(compute),
            },
            gamla.post_json_async(30, f"{base_vault_url}/auth/azure/login"),
            lambda response: response.json(),
            gamla.get_in(["auth", "client_token"]),
        )

    return _pod_identity_token


def _make_vault_headers(
    token: Callable[[], Coroutine[Any, Any, str]],
) -> Callable[[], Coroutine[Any, Any, Dict[str, str]]]:
    async def _vault_headers() -> Dict[str, str]:
        return {
            "X-Vault-Token": await token(),
        }

    return _vault_headers


def _make_read_key(
    base_vault_url: str,
    headers: Callable[[], Coroutine[Any, Any, dict]],
):
    async def read_key(path: str) -> Dict[str, str]:
        return gamla.pipe(
            await gamla.get_async_with_headers(
                (await headers()),
                30,
                f"{base_vault_url}/secret/data/{path}",
            ),
            lambda response: response.json(),
            gamla.excepts(
                KeyError,
                gamla.make_raise(KeyDoesNotExist),
                gamla.get_in(["data", "data"]),
            ),
        )

    return read_key


def _make_write_key(
    base_vault_url: str,
    headers: Callable[[], Coroutine[Any, Any, dict]],
):
    async def write_key(path: str, value: Dict[str, str]):
        return await gamla.post_json_with_extra_headers_async(
            (await headers()),
            30,
            f"{base_vault_url}/secret/data/{path}",
            {"data": value},
        )

    return write_key


def _make_update_key(read_key: Callable, write_key: Callable):
    async def update_key(path: str, value: Dict[str, str]):
        current_value = {}
        try:
            current_value = await read_key(path)
        except KeyDoesNotExist:
            pass
        return await write_key(path, gamla.merge(current_value, value))

    return update_key


def make_vault(host: str, role: str, token: Optional[str]):
    async def get_token():
        return token

    base_vault_url = f"{host}/v1"
    headers = _make_vault_headers(
        get_token if token else _make_pod_identity_token(role, base_vault_url),
    )

    write_key, read_key = _make_write_key(base_vault_url, headers), _make_read_key(
        base_vault_url,
        headers,
    )
    return write_key, read_key, _make_update_key(read_key, write_key)
