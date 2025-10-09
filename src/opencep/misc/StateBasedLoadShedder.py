import heapq
import sys
from collections import defaultdict
from typing import Optional

WINDOW_SECONDS = 3600.0


def slice_id(start_time, last_time):
    age = last_time - start_time
    ratio = age.total_seconds() / WINDOW_SECONDS
    # print(f"slice_id: start {start_time}, last {last_time}, age {age}, ratio {ratio}")
    # 3 equal 20‑min slices in 1 hour
    if ratio < 1 / 3:
        return 0
    if ratio < 2 / 3:
        return 1
    return 2


def length_id(length):
    if length <= 2:
        return 0
    if length <= 5:
        return 1
    return 2


"""
class ExpiryHeap:
    def __init__(self):
        self.heap = []      # (expire_at, partial_id)
        self.alive = set()

    def add(self, partial_id, expire_at):
        heapq.heappush(self.heap, (expire_at, partial_id))
        self.alive.add(partial_id)

    def mark_dead(self, partial_id):
        self.alive.discard(partial_id)

    def evict(self, now_ts, drop_cb):
        while self.heap and self.heap[0][0] <= now_ts:
            _, partial_id = heapq.heappop(self.heap)
            if partial_id in self.alive:
                self.alive.remove(partial_id)
                drop_cb(partial_id)
"""


class BucketStats:
    def __init__(self):
        self.contribution = 0.0
        self.consumption = 0.0


class BucketManager:
    def __init__(self):
        # bucket_id -> set(partial_id)
        self.buckets = defaultdict(set)
        # partial_id -> bucket_id
        self.partials = {}
        # bucket_id -> BucketStats
        self.stats = defaultdict(BucketStats)

    def debug_print_buckets(self, show_stats: bool = True):
        """Print all known buckets and their contents for debugging."""
        if not self.buckets:
            print("BucketManager: no buckets", file=sys.stderr)
            return
        print(
            "BucketManager: listing buckets (bucket_id -> partial_ids)", file=sys.stderr
        )
        for bucket_id, partial_ids in sorted(self.buckets.items()):
            stats = self.stats.get(bucket_id)
            if show_stats and stats is not None:
                print(
                    f"  Bucket {bucket_id}: count={len(partial_ids)}, contribution={stats.contribution:.3f}, consumption={stats.consumption:.3f}",
                    file=sys.stderr,
                )
            else:
                print(
                    f"  Bucket {bucket_id}: count={len(partial_ids)}", file=sys.stderr
                )
            if partial_ids:
                # show ids in deterministic order
                try:
                    pid_list = sorted(partial_ids)
                except Exception:
                    pid_list = list(partial_ids)
                print(
                    "    partial_ids:",
                    ", ".join(str(x) for x in pid_list),
                    file=sys.stderr,
                )

    def add_partial(self, partial_id, slice_id, length_id):
        bucket_id = (slice_id, length_id)
        print(
            f"BucketManager.add_partial called: pid={partial_id}, bucket={bucket_id}",
            file=sys.stderr,
        )
        if partial_id in self.partials:
            # already registered - move if needed
            old_bucket = self.partials[partial_id]
            if old_bucket == bucket_id:
                return
            # remove from old
            self.buckets[old_bucket].discard(partial_id)
            self.stats[old_bucket].consumption += -1.0

        self.buckets[bucket_id].add(partial_id)
        self.partials[partial_id] = bucket_id
        bucket = self.stats[bucket_id]
        bucket.consumption += 1.0  # simple cost increment
        self.debug_print_buckets(show_stats=True)

    def remove_partial(self, partial_id):
        """Remove a partial by id from whichever bucket it's in. Returns True if removed."""
        print(f"BucketManager.remove_partial called: pid={partial_id}", file=sys.stderr)
        bucket_id = self.partials.pop(partial_id, None)
        if bucket_id is None:
            return False
        # discard from bucket set
        self.buckets[bucket_id].discard(partial_id)
        # update stats safely
        stats = self.stats[bucket_id]
        stats.consumption -= 1.0
        return True

    def buckets_sorted_by_value(self, reverse=True):
        """Return list of (bucket_id, BucketStats) sorted by bucket value (contribution/consumption).
        If consumption==0 the value is considered 0. By default returns descending (highest first).
        """

        def value(item):
            stats = item[1]
            return (
                (stats.contribution / stats.consumption) if stats.consumption else 0.0
            )

        return sorted(self.stats.items(), key=value, reverse=reverse)

    def shed_lowest_value_buckets(self, n=1, min_partials_removed=0):
        """Shed the n buckets with the lowest value (contribution/consumption).
        If min_partials_removed>0, continue shedding buckets (starting from lowest value)
        until at least that many partial ids have been freed (or until no buckets left).
        Returns the list of removed partial_ids.
        """
        # sort ascending (lowest value first)
        sorted_buckets = self.buckets_sorted_by_value(reverse=False)
        removed = []
        count = 0
        for bucket_id, _ in sorted_buckets:
            if n <= 0 and min_partials_removed <= 0:
                break
            # skip empty buckets
            partial_ids = list(self.buckets.get(bucket_id, ()))
            if not partial_ids:
                # remove empty stats to keep structures tidy
                self.stats.pop(bucket_id, None)
                self.buckets.pop(bucket_id, None)
                continue
            # remove this bucket entirely
            for partial_id in partial_ids:
                # remove mapping
                self.partials.pop(partial_id, None)
            removed.extend(partial_ids)
            count += len(partial_ids)
            # clear bucket and stats
            self.buckets.pop(bucket_id, None)
            self.stats.pop(bucket_id, None)
            n -= 1
            if min_partials_removed and count >= min_partials_removed:
                break
        return removed

    def shed_by_partial_count(self, target_count: int):
        """FOR DEBUGGING! Shed individual partial ids starting from the lowest-value buckets.
        Returns the list of removed partial ids.
        """
        if target_count <= 0:
            return []
        removed = []
        count = 0
        # iterate buckets from lowest value first (snapshot the order)
        for bucket_id, _ in self.buckets_sorted_by_value(reverse=False):
            # get a deterministic list of pids for this bucket
            pids = list(self.buckets.get(bucket_id, ()))
            if not pids:
                # tidy empty bucket
                self.stats.pop(bucket_id, None)
                self.buckets.pop(bucket_id, None)
                continue
            try:
                pids = sorted(pids)
            except Exception:
                # fall back to insertion order
                pids = list(pids)

            # remove individual partials from this bucket until target reached
            for pid in pids:
                if count >= target_count:
                    break
                # remove mapping and bucket membership
                self.partials.pop(pid, None)
                self.buckets[bucket_id].discard(pid)
                removed.append(pid)
                count += 1
                # update stats consumption safely
                stats = self.stats.get(bucket_id)
                if stats is not None:
                    stats.consumption -= 1.0

            # if bucket emptied, remove its structures
            if not self.buckets.get(bucket_id):
                self.buckets.pop(bucket_id, None)
                self.stats.pop(bucket_id, None)

            if count >= target_count:
                break

        return removed


# Module-level bucket manager instance (single source of truth)
bucket_manager = BucketManager()
