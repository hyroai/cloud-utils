import hvac

_URL = "URL"
_TOKEN = "TOKEN"


def _get_client(url: str, token: str) -> hvac.Client:
    return hvac.Client(url=url, token=token)


def read_all_secrets(path: str) -> dict[str, str]:
    client = _get_client(_URL, _TOKEN)
    read_secrets = client.secrets.kv.v2.read_secret_version(path=path)
    return read_secrets["data"]["data"]


# TODO: do we want to have the ability to read a specific secret?
# def read_secret(path: str, key: str) -> str:
#     secrets_data = read_all_secrets(path)
#     return secrets_data[key]


def write_or_update_secrets(path: str, secrets: dict[str, str], mount_point: str):
    client = _get_client(_URL, _TOKEN)
    secrets_data = read_all_secrets(path)
    secrets_data.update(secrets)
    client.secrets.kv.v2.create_or_update_secret(
        path=path,
        secret=secrets_data,
        mount_point=mount_point,
    )


def make_vault():
    return write_or_update_secrets, read_all_secrets
