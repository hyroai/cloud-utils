import asyncio
import logging
import os
import tempfile
from typing import Any, Dict, Text

import gamla
import yaml


class _HelmException(Exception):
    pass


async def _run_in_shell(args, path: Text) -> Text:
    logging.info(f"Running shell command: {args}")
    process = await asyncio.subprocess.create_subprocess_shell(
        cmd=" ".join(args),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=path,
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        raise _HelmException(stderr.decode("utf-8"))
    return stdout.decode("utf-8")


async def get_releases():
    return gamla.pipe(
        await _run_in_shell(["helm", "list", "-q"], "./"),
        lambda output: output.split("\n"),
        frozenset,
    )


async def install_release(
    chart_name: Text,
    release_name: Text,
    chart_values: Dict[Text, Any],
    chart_physical_dir: Text,
):
    handle, filename = tempfile.mkstemp()
    del handle
    try:
        with open(filename, "w") as values_file:
            values_file.write(yaml.dump(chart_values))
        await _run_in_shell(
            ["helm", "upgrade", release_name, chart_name, "--install", "-f", filename],
            chart_physical_dir,
        )
    finally:
        os.remove(filename)


async def delete_release(release_name: Text):
    try:
        await _run_in_shell(["helm", "uninstall", release_name], "./")
    except _HelmException:
        logging.debug(f"Unable to delete release {release_name}")
