import heapq
from collections import defaultdict

WINDOW_SECONDS = 3600.0

def slice_id(start_time, last_time):
    age = last_time - start_time
    r = age / WINDOW_SECONDS
    # 3 equal 20‑min slices in 1 hour
    return 0 if r < 1/3 else (1 if r < 2/3 else 2)

def length_bucket(length):
    if length <= 2: return 0
    if length <= 5: return 1
    return 2

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

class BucketStats:
    __slots__ = ("gamma_plus","gamma_minus","active")
    def __init__(self):
        self.gamma_plus = 0.0
        self.gamma_minus = 0.0
        self.active = 0

class BucketManager:
    def __init__(self):
        self.partials = {}
        self.buckets = defaultdict(set)
        self.stats = defaultdict(BucketStats)

    def add_partial(self, partial_id, s, lb):
        self.partials[partial_id] = (s, lb)
        self.buckets[(s, lb)].add(partial_id)
        st = self.stats[(s, lb)]
        st.active += 1
        st.gamma_minus += 1.0  # simple cost increment per live partial

    def drop_partial(self, partial_id):
        info = self.partials.pop(partial_id, None)
        if not info:
            return
        s, lb = info
        self.buckets[(s, lb)].discard(partial_id)
        self.stats[(s, lb)].active -= 1

    def move_if_needed(self, partial_id, new_s, new_lb):
        old = self.partials.get(partial_id)
        if not old:
            return
        os, olb = old
        if (os, olb) == (new_s, new_lb):
            return
        self.buckets[(os, olb)].discard(partial_id)
        self.stats[(os, olb)].active -= 1
        self.buckets[(new_s, new_lb)].add(partial_id)
        self.stats[(new_s, new_lb)].active += 1
        self.partials[partial_id] = (new_s, new_lb)

    def shed(self):
        print("placeholder")