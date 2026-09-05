from dataclasses import dataclass, asdict


@dataclass
class IndexedDocument:
    document_id: str
    url: str
    title: str
    text: str
    content_hash: str = ""
    canonical_url: str = ""
    status: str = "active"

    def to_dict(self):
        return asdict(self)
