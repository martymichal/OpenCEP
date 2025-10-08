from opencep.base.Event import Event
from typing import List


class PatternMatch:
    """
    Represents a set of primitive events satisfying one or more patterns.
    An instance of this class could correspond either to a full pattern match, or to any intermediate result
    created during the evaluation process.
    """

    # Count of all pattern matches created
    counter = 0

    def __init__(self, events: List[Event], probability: float = None):
        self.events = events
        self.last_timestamp = max(events, key=lambda x: x.max_timestamp).max_timestamp
        self.first_timestamp = min(events, key=lambda x: x.min_timestamp).min_timestamp
        # this field is only used for full pattern matches
        self.pattern_ids = []
        self.probability = probability
        # Increment global counter to gain a unique ID for this pattern match
        PatternMatch.counter += 1
        self.partial_id = PatternMatch.counter

    def __eq__(self, other):
        return isinstance(other, PatternMatch) and set(self.events) == set(other.events) and \
               self.pattern_ids == other.pattern_ids

    def __str__(self):
        result = ""
        match = ""
        for event in self.events:
            match += "%s\n" % event
        if len(self.pattern_ids) == 0:
            result += match
            result += "\n"
        else:
            for idx in self.pattern_ids:
                result += "%s: " % idx
                result += match
                result += "\n"
        return result

    def add_pattern_id(self, pattern_id: int):
        """
        Adds a new pattern ID corresponding to this pattern,
        """
        if pattern_id not in self.pattern_ids:
            self.pattern_ids.append(pattern_id)
