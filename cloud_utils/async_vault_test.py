import json

import pytest

from cloud_utils import async_vault, vault_shared

_HOST = "http://vault:8200"


async def test_read_secret_token_auth(httpx_mock):
    httpx_mock.add_response(
        url=f"{_HOST}/v1/secret/data/a/b",
        json={"data": {"data": {"k": "v"}, "metadata": {}}},
        match_headers={"X-Vault-Token": "tok"},
    )
    read_secret, _ = await async_vault.make_vault_async(_HOST, None, "tok")
    assert await read_secret("a/b", None) == {"k": "v"}


async def test_read_secret_invalid_path(httpx_mock):
    httpx_mock.add_response(
        url=f"{_HOST}/v1/secret/data/missing",
        status_code=404,
        json={"errors": []},
    )
    read_secret, _ = await async_vault.make_vault_async(_HOST, None, "tok")
    with pytest.raises(vault_shared.InvalidVaultPathError):
        await read_secret("missing", None)


async def test_write_or_update_merges_existing(httpx_mock):
    httpx_mock.add_response(
        url=f"{_HOST}/v1/secret/data/p",
        json={"data": {"data": {"a": "1"}, "metadata": {}}},
    )
    httpx_mock.add_response(url=f"{_HOST}/v1/secret/data/p", method="POST", json={})
    _, write_secret = await async_vault.make_vault_async(_HOST, None, "tok")
    await write_secret("p", {"b": "2"})
    post_request = [r for r in httpx_mock.get_requests() if r.method == "POST"][0]
    assert json.loads(post_request.content) == {"data": {"a": "1", "b": "2"}}


async def test_k8s_login(httpx_mock, monkeypatch, tmp_path):
    jwt_file = tmp_path / "token"
    jwt_file.write_text("jwt-value")
    monkeypatch.setattr(vault_shared, "K8S_JWT_PATH", str(jwt_file))
    httpx_mock.add_response(
        url=f"{_HOST}/v1/auth/kubernetes/login",
        json={"auth": {"client_token": "t"}},
    )
    httpx_mock.add_response(
        url=f"{_HOST}/v1/secret/data/p",
        json={"data": {"data": {}, "metadata": {}}},
    )
    read_secret, _ = await async_vault.make_vault_async(_HOST, "my-role", None)
    await read_secret("p", None)
