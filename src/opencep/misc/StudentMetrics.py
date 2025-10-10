import time
from enum import StrEnum
from typing import Any
from threading import Lock


class Metrics(StrEnum):
    EVENT_PROCESSING_LATENCY = "event_processing_latency"
    PROCESSED_EVENTS = "processed_events"
    DETECTED_MATCHES = "detected_matches"


class Fields(StrEnum):
    TIME = "time"
    TYPE = "type"
    METRIC = "metric"
    VALUE = "value"
    ATTRIBUTE = "attribute"
    ATTRIBUTE_VALUE = "attribute_value"


last_values = {
    Metrics.EVENT_PROCESSING_LATENCY: 0,
}
lock = Lock()

test_last_values = {
    Metrics.PROCESSED_EVENTS: 0,
}


def _log_metric(values: list[str]):
    assert len(values) == len(Fields)
    lock.acquire()
    print(" ".join([str(v) for v in values]), flush=True)
    lock.release()


def increment_counter(metric: Metrics, cur_time: int = 0):
    assert isinstance(metric, Metrics)
    cur_time = time.perf_counter_ns if cur_time == 0 else cur_time
    _log_metric([cur_time, "counter", str(metric), 1, 0, 0])

    # for testing, should not affect existing functionality
    if metric in test_last_values:
        test_last_values[metric] += 1
        #print(f"Test metric {metric} incremented to {test_last_values[metric]}")


def mark_hist_point(metric: Metrics, value, attrs: dict[str, Any], cur_time: int = 0):
    assert isinstance(metric, Metrics)
    cur_time = time.perf_counter_ns if cur_time == 0 else cur_time
    _log_metric(
        [cur_time, "hist", str(metric), int(value), list(attrs.keys())[0], list(attrs.values())[0]]
    )
    last_values[metric] = value
