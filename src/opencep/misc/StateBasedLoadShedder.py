import datetime
import sys
from collections import defaultdict

WINDOW_SECONDS = 3600.0
NUM_OF_TIME_SLICES = 3


def slice_id(start_time: datetime.datetime, last_time: datetime.datetime):
    age = last_time - start_time
    ratio = age.total_seconds() / WINDOW_SECONDS
    for i in range(1, NUM_OF_TIME_SLICES - 1):
        if ratio < i / NUM_OF_TIME_SLICES:
            return i - 1
    return NUM_OF_TIME_SLICES - 1

def length_id(length: int):  # inverts the length value to favor longer matches
    if length <= 1:
        return 2
    elif length <= 2:
        return 1
    else:
        return 0

class BucketStats:
    def __init__(self):
        self.contribution = 0.0
        self.consumption = 0.0
        self.active_partials = 0  # number of partials currently in this bucket


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
                    f"  Bucket {bucket_id}: count={stats.active_partials}",
                    file=sys.stderr,
                )
            else:
                print(
                    f"  Bucket {bucket_id}: count(partial_ids)={len(partial_ids)}", file=sys.stderr
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
        """print(
            f"BucketManager.add_partial called: pid={partial_id}, bucket={bucket_id}",
            file=sys.stderr,
        )"""
        if partial_id in self.partials:
            # already registered - move if needed
            old_bucket = self.partials[partial_id]
            if old_bucket == bucket_id:
                return
            # remove from old
            self.buckets[old_bucket].discard(partial_id)
            self.stats[old_bucket].consumption -= 1.0
            self.stats[old_bucket].active_partials -= 1

        self.buckets[bucket_id].add(partial_id)
        self.partials[partial_id] = bucket_id
        bucket = self.stats[bucket_id]
        bucket.consumption += 1.0  # simple cost increment
        bucket.active_partials += 1  # increment active partials
        #self.debug_print_buckets(show_stats=True)

    def remove_partial(self, partial_id):
        """Remove a partial by id from whichever bucket it's in. Returns True if removed."""
        #print(f"BucketManager.remove_partial called: pid={partial_id}", file=sys.stderr)
        bucket_id = self.partials.pop(partial_id, None)
        if bucket_id is None:
            return False
        # discard from bucket set
        self.buckets[bucket_id].discard(partial_id)
        # update stats safely
        stats = self.stats[bucket_id]
        stats.consumption -= 1.0
        stats.active_partials -= 1

        return True

    def buckets_sorted_by_value(self, reverse=True):
        """Return list of (bucket_id, BucketStats) sorted by bucket value (contribution/consumption).
        If consumption==0 the value is considered 0. By default returns descending (highest first).
        """

        def value(item):
            bucket_id = item[0]
            try:
                slice_idx, length_idx = bucket_id
                return float(slice_idx + length_idx)
            except Exception:
                return 0.0

        return sorted(self.stats.items(), key=value, reverse=reverse)

    #def shed_lowest_value_buckets(self, n=1, min_partials_removed=0):
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
    
    def shed_highest_value_buckets(self, n=1, min_partials_removed=0):
        """Shed the n buckets with the highest value.
        If min_partials_removed>0, continue shedding buckets (starting from highest value)
        until at least that many partial ids have been freed (or until no buckets left).
        Returns the list of removed partial_ids.
        """
        # sort descending (highest value first)
        sorted_buckets = self.buckets_sorted_by_value(reverse=True)
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
