import contextlib
import contextvars
import logging
import os
import sys
from typing import Iterator

_logging_prefix: contextvars.ContextVar[str] = contextvars.ContextVar(
    "logging_prefix",
    default="",
)


@contextlib.contextmanager
def logging_prefix(prefix: str) -> Iterator[str]:
    token = _logging_prefix.set(prefix)
    try:
        yield prefix
    finally:
        _logging_prefix.reset(token)


def _remove_all_handlers() -> None:
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)


def _context_filter(record) -> int:
    record.prefix = _logging_prefix.get()
    record.environment = os.getenv("HOSTNAME", "dev")
    return True


class _MultilineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord):
        save_msg = str(record.msg)
        output = []

        for line in save_msg.splitlines():
            record.msg = line
            output.append(super().format(record))

        output_str = "\n".join(output)
        record.msg = save_msg
        record.message = output_str
        return output_str


def initialize_logger() -> None:
    # TODO(erez): change to force=True once we update python version to 3.8.
    _remove_all_handlers()
    formatter = _MultilineFormatter(
        "[%(environment)s] [%(prefix)s] [%(asctime)s,%(msecs)d] [%(levelname)-8s] [%(filename)s:%(lineno)d] %(message)s",
        datefmt="%d-%m-%Y:%H:%M:%S",
    )
    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    stdout_handler.setFormatter(formatter)
    stdout_handler.addFilter(_context_filter)

    logging.basicConfig(level=logging.INFO, handlers=[stdout_handler])


# This needs to be before other imports otherwise not logging to stdout for some reason.
initialize_logger()
