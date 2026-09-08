import hashlib


class DocumentIdentity:
    PREFIX = "DOC"

    @classmethod
    def from_url(cls, url):
        if not isinstance(url, str):
            raise TypeError("url must be a string")

        normalized = url.strip()

        if not normalized:
            raise ValueError("url must be a non-empty string")

        digest = hashlib.sha256(
            normalized.encode("utf-8")
        ).hexdigest()

        return f"{cls.PREFIX}-{digest}"

    @classmethod
    def from_normalized_url(cls, normalized_url):
        return cls.from_url(normalized_url)
