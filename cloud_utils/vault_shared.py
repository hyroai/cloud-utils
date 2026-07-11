MOUNT_POINT = "secret"
K8S_JWT_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"


class InvalidVaultPathError(Exception):
    pass