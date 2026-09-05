import time


class IndexMaintenance:
    def __init__(self, segment_manager):
        self.segment_manager = segment_manager

        self.stats = {
            "merge_runs": 0,
            "segments_merged": 0,
            "segments_removed": 0,
        }

    def segment_count(self):
        return self.segment_manager.segment_count()

    def list_segments(self):
        return self.segment_manager.list_segments()

    def segment_ids(self):
        return [
            item["segment_id"]
            for item in self.list_segments()
        ]

    def should_merge(self, max_segments=10):
        return self.segment_count() > max_segments

    def merge_all(self):
        segments = self.segment_ids()

        if len(segments) <= 1:
            return None

        before = len(segments)

        segment_id = self.segment_manager.merge_segments(
            segments
        )

        after = self.segment_count()

        self.stats["merge_runs"] += 1
        self.stats["segments_merged"] += before
        self.stats["segments_removed"] += max(
            0,
            before - after
        )

        return segment_id

    def status(self):
        return {
            "segments": self.segment_count(),
            "should_merge": self.should_merge(),
            "stats": dict(self.stats),
            "checked_at": time.time(),
        }
