import time
from enum import StrEnum
from typing import Any


class Metrics(StrEnum):
    EVENT_PROCESSING_LATENCY = "event_processing_latency"
    PROCESSED_EVENTS = "processed_events"


_metrics = {
    Metrics.EVENT_PROCESSING_LATENCY: [],
    Metrics.PROCESSED_EVENTS: 0,
}


def increment_counter(metric: Metrics, cur_time: int=0):
    assert isinstance(metric, Metrics)
    cur_time = time.perf_counter_ns if cur_time == 0 else cur_time
    _metrics[metric] += 1
    print(f"{cur_time} counter {str(metric)} {_metrics[metric]}", flush=True)


def mark_hist_point(metric: Metrics, value, attrs: dict[str, Any], cur_time: int=0):
    assert isinstance(metric, Metrics)
    cur_time = time.perf_counter_ns if cur_time == 0 else cur_time
    _metrics[metric] += (value, attrs)
    attrs_str = " ".join([f"{key} {val}" for key, val in attrs.items()])
    print(f"{cur_time} hist {str(metric)} {value} {attrs_str}", flush=True)

