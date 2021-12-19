import traceback
from typing import Dict

import gamla

send_message_to_webhook = gamla.post_json_async(5)


def _make_exception_payload(text: str) -> Dict:
    """Assumes being called from within a caught exception context."""
    return {
        "text": text,
        "attachments": [
            {
                "fields": [
                    {
                        "title": "Exception",
                        "short": False,
                        "value": gamla.pipe(
                            traceback.format_exc(),
                            str.splitlines,
                            reversed,
                            "\n".join,
                        ),
                    },
                ],
            },
        ],
    }


async def report_exception(webhook_url: str, text: str):
    await gamla.pipe(
        text,
        _make_exception_payload,
        gamla.post_json_async(5, webhook_url),
    )
