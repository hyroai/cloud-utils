import base64
import hashlib
import logging
import timeit
from typing import Text, Tuple

import dill
import gamla
import redis
from nlu import agent, config, user_utterance

_REDIS_CLIENT = redis.Redis(
    host=config.AGENT_STATE_CACHE_REDIS_HOST,
    port=6379,
    db=0,
    decode_responses=True,
    password=config.AGENT_STATE_CACHE_REDIS_PASSWORD,
)


def _is_enabled():
    return config.ENVIRONMENT in ["staging", "production"]


def _encode_agent_state(agent_state: agent.AgentState) -> Text:
    return base64.b64encode(dill.dumps(agent_state)).decode(encoding="UTF-8")


def _decode_agent_state(state_bytes: bytes) -> agent.AgentState:
    decoded = dill.loads(base64.b64decode(state_bytes))
    assert isinstance(decoded, agent.AgentState)
    return decoded


@gamla.curry
def _get_utterance_list_hash(
    utterances: Tuple[user_utterance.UserUtterance, ...], unique_id: Text
) -> Text:
    return hashlib.sha1(
        (unique_id + str(len(utterances))).encode(encoding="UTF-8")
    ).hexdigest()


def get_agent_state_for_utterances(
    utterances: Tuple[user_utterance.UserUtterance, ...], unique_id: Text
) -> agent.AgentState:
    if not _is_enabled():
        raise KeyError

    load_start = timeit.default_timer()

    try:
        state_bytes = _REDIS_CLIENT.get(
            _get_utterance_list_hash(utterances=utterances, unique_id=unique_id)
        )
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
        logging.error("Could not load bot state, redis not available.")
        raise KeyError

    if state_bytes is None:
        logging.warning(
            "State is missing in redis (this should happen only if there was a recent release of new version)."
        )
        raise KeyError

    logging.info(
        f"Loaded state from redis for {','.join(x.utterance for x in utterances)} in {timeit.default_timer() - load_start}s."
    )
    return _decode_agent_state(state_bytes)


def save_agent_state_for_utterances(
    utterances: Tuple[user_utterance.UserUtterance, ...],
    agent_state: agent.AgentState,
    unique_id: Text,
):
    if not _is_enabled():
        return

    try:
        save_start = timeit.default_timer()
        _REDIS_CLIENT.set(
            _get_utterance_list_hash(utterances=utterances, unique_id=unique_id),
            _encode_agent_state(agent_state),
            ex=60 * 60 * 2,  # Every two hours.
        )
        logging.info(
            f"Saved state in redis for {','.join(x.utterance for x in utterances)} in {timeit.default_timer() - save_start}s."
        )
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
        logging.error("Could not save agent_state in redis, redis is not available.")
