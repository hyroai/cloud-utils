import traceback
from typing import Dict, Optional, Text

import gamla
import requests


def send_message_to_webhook(webhook_url: Text, text: Text, data: Optional[Dict]):
    message = {"text": text}
    if data is not None:
        message.update(data)
    requests.post(webhook_url, json=message)


def report_exception(webhook_url: Text, text: Text):
    """Assumes being called from within a caught exception context."""
    send_message_to_webhook(
        webhook_url,
        text,
        data={
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
        },
    )
