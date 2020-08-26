import base64
import os
from pathlib import Path

from kubernetes import config


def init_kubernetes_client():
    kube_config_file = Path("~/.kube/config").expanduser()
    if not kube_config_file.exists():
        config_decoded = base64.b64decode(os.environ["KUBE_CONFIG"])
        Path("~/.kube").expanduser().mkdir(exist_ok=True)
        kube_config_file.touch(exist_ok=True)
        kube_config_file.write_bytes(config_decoded)
    config.load_kube_config()