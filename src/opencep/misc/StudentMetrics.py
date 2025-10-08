import time
from enum import StrEnum
from typing import Any
from threading import Lock


class Metrics(StrEnum):
    EVENT_PROCESSING_LATENCY = "event_processing_latency"
    PROCESSED_EVENTS = "processed_events"


_metrics = {
    Metrics.EVENT_PROCESSING_LATENCY: [],
    Metrics.PROCESSED_EVENTS: 0,
}

lock = Lock()


def _log_metric(msg: str):
    lock.acquire()
    print(msg, flush=True)
    lock.release()


def increment_counter(metric: Metrics, cur_time: int = 0):
    assert isinstance(metric, Metrics)
    cur_time = time.perf_counter_ns if cur_time == 0 else cur_time
    _metrics[metric] += 1
    _log_metric(f"{cur_time} counter {str(metric)} 1")


def mark_hist_point(metric: Metrics, value, attrs: dict[str, Any], cur_time: int = 0):
    assert isinstance(metric, Metrics)
    cur_time = time.perf_counter_ns if cur_time == 0 else cur_time
    _metrics[metric] += (value, attrs)
    attrs_str = " ".join([f"{key} {val}" for key, val in attrs.items()])
    _log_metric(f"{cur_time} hist {str(metric)} {value} {attrs_str}")
