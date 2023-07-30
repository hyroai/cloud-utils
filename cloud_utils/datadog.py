import asyncio
import functools
import logging
import time
from datetime import datetime
from typing import Callable

import gamla
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.exceptions import ApiException
from datadog_api_client.v1.api.events_api import EventsApi
from datadog_api_client.v1.model.event_create_request import EventCreateRequest
from datadog_api_client.v2.api.metrics_api import MetricsApi
from datadog_api_client.v2.model.metric_intake_type import MetricIntakeType
from datadog_api_client.v2.model.metric_payload import MetricPayload
from datadog_api_client.v2.model.metric_point import MetricPoint
from datadog_api_client.v2.model.metric_resource import MetricResource
from datadog_api_client.v2.model.metric_series import MetricSeries

configuration = Configuration()

Tags = gamla.Enum(["job_id", "status_code", "scraper", "function_name", "success"])


def send_event(conf: dict, title: str, text: str, tags: list[str]) -> None:
    configuration.api_key["apiKeyAuth"] = conf["DATADOG_API_KEY"]
    configuration.api_key["appKeyAuth"] = conf["DATADOG_APP_KEY"]
    with ApiClient(configuration) as api_client:
        api_instance = EventsApi(api_client)
        body = EventCreateRequest(title=title, text=text, tags=tags)
        try:
            api_instance.create_event(body)
        except ApiException as e:
            logging.error(f"Exception when sending event to datadog {e}")


def _build_metric_body(
    metric_name: str,
    value: float,
    tags: list[str],
) -> MetricPayload:
    return MetricPayload(
        series=[
            MetricSeries(
                metric=metric_name,
                points=[
                    MetricPoint(value=value, timestamp=int(datetime.now().timestamp())),
                ],
                tags=tags,
                type=MetricIntakeType.GAUGE,
                resources=[MetricResource(name="nlu-runtime", type="host")],
            ),
        ],
    )


def send_metric(conf: dict, metric_name: str, value: float, tags: list[str]) -> None:
    configuration.api_key["apiKeyAuth"] = conf["DATADOG_API_KEY"]
    configuration.api_key["appKeyAuth"] = conf["DATADOG_APP_KEY"]
    with ApiClient(configuration) as api_client:
        api_instance = MetricsApi(api_client)
        body = _build_metric_body(metric_name, value, tags)
        try:
            api_instance.submit_metrics(body)
        except ApiException as e:
            logging.error(f"Exception when sending metric to datadog {e}")


def send_duration_metric(conf: dict, elapsed_time: str, function_name: str) -> None:
    send_metric(
        conf,
        "function_duration",
        float(elapsed_time),
        [f"{Tags.function_name}:{function_name}"],
    )


def _async_timeit_with_metric(conf: dict, f: Callable) -> Callable:
    @functools.wraps(f)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        result = await f(*args, **kwargs)
        elapsed_time = str(round(time.time() - start_time, 2))
        gamla.log_finish(f.__name__, start_time)
        send_duration_metric(conf, elapsed_time, f.__name__)
        return result

    return wrapper


@gamla.curry
def timeit_with_metric(conf: dict, f: Callable) -> Callable:
    if asyncio.iscoroutinefunction(f):
        return _async_timeit_with_metric(conf, f)

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = f(*args, **kwargs)
        elapsed_time = str(round(time.time() - start_time, 2))
        gamla.log_finish(f.__name__, start_time)
        send_duration_metric(conf, elapsed_time, f.__name__)
        return result

    return wrapper
