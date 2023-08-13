import logging
import os
import traceback
from typing import Callable, Dict

import gamla
import slack_sdk
from gamla import frozendict
from slack_sdk import errors as slack_errors

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


def _send_dm(client: slack_sdk.WebClient, message: str) -> Callable[[str], None]:
    def send_dm(channel: str):
        client.chat_postMessage(channel=channel, text=message)

    return send_dm


def _get_slack_user(
    client: slack_sdk.WebClient,
    email: str,
    email_map: Dict[str, str],
) -> slack_sdk.web.SlackResponse:
    try:
        return client.users_lookupByEmail(email=email)
    except slack_errors.SlackApiError:
        return client.users_lookupByEmail(email=email_map.get(email, email))


def dm_slack_user(email: str, message: str, email_map: Dict[str, str] = frozendict()):
    client = slack_sdk.WebClient(token=os.environ["SLACK_DM_TOKEN"])
    gamla.pipe(
        _get_slack_user(client, email, email_map),
        gamla.get_in(["user", "id"]),
        _send_dm(client, message),
    )
