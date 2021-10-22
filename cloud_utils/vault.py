import os
from typing import Dict

import gamla
from cache import AsyncTTL

base_vault_url = f'{os.getenv("VAULT_HOST")}/v1'
base_metadata_url = "http://169.254.169.254/metadata"


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


@AsyncTTL(time_to_live=60 * 60 * 23, maxsize=1)  # Vault token is valid for 24 hour.
async def _vault_headers():
    jwt = await _identity_token()
    token = await gamla.pipe(
        await _instance_metadata(),
        gamla.get_in(["compute"]),
        lambda compute: {
            "role": os.getenv("ROLE") or f'{os.getenv("VAULT_KEY")}-role',
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

    return {"X-Vault-Token": token}


async def read_key(path: str) -> Dict[str, str]:
    return gamla.pipe(
        await gamla.get_async_with_headers(
            (await _vault_headers()),
            30,
            f"{base_vault_url}/secret/data/{path}",
        ),
        lambda response: response.json(),
        gamla.get_in(["data", "data"]),
    )


async def write_key(path: str, value: Dict[str, str]):
    return await gamla.post_json_with_extra_headers_async(
        (await _vault_headers()),
        30,
        f"{base_vault_url}/secret/data/{path}",
        {"data": value},
    )
