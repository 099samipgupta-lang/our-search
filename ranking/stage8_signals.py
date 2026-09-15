import hashlib
import math
import re
import time


class Stage8SignalEngine:
    """
    Deterministic, bounded quality/freshness/trust/spam/duplicate signals.

    Signals are intentionally intrinsic to the indexed document. They do not
    depend on Google, external ranking APIs, or opaque third-party scores.
    """

    FORMAT_VERSION = 1

    _WORD_RE = re.compile(r"[a-zA-Z0-9]+")

    def __init__(self, now=None):
        self.now = float(time.time() if now is None else now)

    @staticmethod
    def _words(text):
        return Stage8SignalEngine._WORD_RE.findall(
            str(text or "").lower()
        )

    @staticmethod
    def _normalized_url(url):
        return str(url or "").strip().lower()

    def quality(self, title, text):
        title = str(title or "").strip()
        text = str(text or "").strip()

        words = self._words(text)
        word_count = len(words)

        score = 0.0

        if title:
            score += 0.20

        if word_count >= 20:
            score += 0.20
        if word_count >= 100:
            score += 0.15
        if word_count >= 300:
            score += 0.15
        if word_count >= 800:
            score += 0.10

        unique_words = len(set(words))

        if word_count:
            diversity = unique_words / word_count
            score += min(0.20, diversity * 0.20)

        return max(0.0, min(1.0, score))

    def spam(self, title, text, url=""):
        title = str(title or "").strip().lower()
        text = str(text or "").strip().lower()
        url = self._normalized_url(url)

        words = self._words(text)

        if not words:
            return 0.0

        counts = {}
        for word in words:
            counts[word] = counts.get(word, 0) + 1

        top_frequency = max(counts.values())
        repetition_ratio = top_frequency / len(words)

        repeated_phrase = (
            len(words) >= 12
            and repetition_ratio >= 0.25
        )

        suspicious_url = any(
            marker in url
            for marker in (
                "casino",
                "betting",
                "viagra",
                "payday-loan",
            )
        )

        excessive_title = len(title.split()) > 25

        signal = 0.0

        if repetition_ratio >= 0.40:
            signal += 0.65
        elif repetition_ratio >= 0.25:
            signal += 0.35

        if repeated_phrase:
            signal += 0.15

        if suspicious_url:
            signal += 0.10

        if excessive_title:
            signal += 0.10

        return max(0.0, min(1.0, signal))

    def trust(self, url, title, text):
        url = self._normalized_url(url)

        score = 0.45

        if url.startswith("https://"):
            score += 0.20

        if title:
            score += 0.10

        if len(self._words(text)) >= 100:
            score += 0.10

        if url:
            score += 0.05

        # Intrinsic consistency: extremely malformed URLs receive less trust.
        if "://" not in url or " " in url:
            score -= 0.20

        return max(0.0, min(1.0, score))

    def freshness(self, last_crawled=None, half_life_days=30.0):
        if last_crawled is None:
            return 0.50

        try:
            age = max(0.0, self.now - float(last_crawled))
        except (TypeError, ValueError):
            return 0.50

        half_life = max(1.0, float(half_life_days)) * 86400.0

        return max(
            0.0,
            min(
                1.0,
                math.pow(0.5, age / half_life),
            ),
        )

    @staticmethod
    def duplicate_penalty(exact_duplicate=False, possible_duplicate=False):
        if exact_duplicate:
            return 1.0
        if possible_duplicate:
            return 0.45
        return 0.0

    @staticmethod
    def content_fingerprint(title, text, url):
        normalized = " ".join(
            str(text or "").lower().split()
        )

        payload = (
            str(title or "").strip().lower()
            + "\n"
            + normalized
            + "\n"
            + str(url or "").strip().lower()
        )

        return hashlib.sha256(
            payload.encode("utf-8")
        ).hexdigest()

    def compute(
        self,
        title,
        text,
        url,
        last_crawled=None,
        exact_duplicate=False,
        possible_duplicate=False,
        removed=False,
    ):
        if removed:
            return {
                "format_version": self.FORMAT_VERSION,
                "freshness": 0.0,
                "quality": 0.0,
                "spam": 1.0,
                "trust": 0.0,
                "duplicate_penalty": 1.0,
                "active": False,
            }

        return {
            "format_version": self.FORMAT_VERSION,
            "freshness": self.freshness(last_crawled),
            "quality": self.quality(title, text),
            "spam": self.spam(title, text, url),
            "trust": self.trust(url, title, text),
            "duplicate_penalty": self.duplicate_penalty(
                exact_duplicate,
                possible_duplicate,
            ),
            "active": True,
        }
