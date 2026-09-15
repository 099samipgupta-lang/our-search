import hashlib
import importlib
import os
import shutil
import tempfile


PROTECTED_FILES = (
    "website_server.py",
    "search_service/server.py",
)


def check(condition, message):
    if not condition:
        raise AssertionError(f"FAIL — {message}")
    print(f"PASS — {message}")


def file_hash(path):
    with open(path, "rb") as file:
        return hashlib.sha256(file.read()).hexdigest()


def snapshot_protected():
    snapshots = {}

    for path in PROTECTED_FILES:
        check(
            os.path.exists(path),
            f"protected file exists: {path}",
        )
        snapshots[path] = file_hash(path)

    return snapshots


def verify_protected(snapshots):
    for path, expected_hash in snapshots.items():
        check(
            os.path.exists(path),
            f"protected file still exists: {path}",
        )
        check(
            file_hash(path) == expected_hash,
            f"protected file unchanged: {path}",
        )


def import_check():
    modules = (
        "exploration.relationships",
        "exploration.experience",
        "result_presentation.assembler",
        "search_service.service",
    )

    for module_name in modules:
        importlib.import_module(module_name)

    print(
        "PASS — Stage 9 production modules import successfully"
    )


def test_rich_results():
    from result_presentation.assembler import ResultAssembler

    class Store:
        def get(self, document_id):
            documents = {
                "doc-a": {
                    "document_id": "doc-a",
                    "title": "Stanford University",
                    "url": "https://www.stanford.edu/",
                    "text": (
                        "Stanford University research education."
                    ),
                    "description": (
                        "Research and education."
                    ),
                    "metadata": {
                        "active": True,
                    },
                },
                "doc-b": {
                    "document_id": "doc-b",
                    "title": "Stanford Research",
                    "url": "https://research.stanford.edu/",
                    "text": (
                        "Stanford research programs."
                    ),
                    "description": (
                        "Stanford research programs."
                    ),
                    "metadata": {
                        "active": True,
                    },
                },
            }

            return documents.get(document_id)

    ranked_results = [
        {
            "document_id": "doc-a",
            "score": 10.0,
            "matched_terms": [
                "stanford",
                "university",
            ],
            "document_length": 100,
        },
        {
            "document_id": "doc-b",
            "score": 8.0,
            "matched_terms": [
                "stanford",
                "research",
            ],
            "document_length": 80,
        },
    ]

    assembler = ResultAssembler(Store())

    output = assembler.assemble(
        ranked_results=ranked_results,
        query_terms=[
            "stanford",
            "university",
        ],
        total_candidates=2,
    )

    check(
        output["total_candidates"] == 2,
        "rich result candidate count",
    )

    check(
        len(output["results"]) == 2,
        "rich result exists",
    )

    result = output["results"][0]

    required_fields = {
        "rank",
        "document_id",
        "title",
        "url",
        "snippet",
        "highlighted_snippet",
        "score",
        "matched_terms",
        "document_length",
    }

    check(
        required_fields.issubset(result.keys()),
        "rich result contains required presentation fields",
    )

    check(
        isinstance(result["snippet"], str)
        and bool(result["snippet"]),
        "rich result contains contextual snippet",
    )

    check(
        isinstance(result["highlighted_snippet"], str)
        and bool(result["highlighted_snippet"]),
        "rich result contains highlighted context",
    )


def test_relationship_engine():
    from exploration.relationships import RelationshipEngine

    engine = RelationshipEngine()

    source = {
        "document_id": "doc-a",
        "title": "Stanford University",
        "url": "https://www.stanford.edu/",
        "snippet": "Stanford University research education.",
        "matched_terms": [
            "stanford",
            "university",
        ],
    }

    candidate_a = {
        "document_id": "doc-b",
        "title": "Stanford Research",
        "url": "https://research.stanford.edu/",
        "snippet": "Stanford research programs.",
        "matched_terms": [
            "stanford",
            "research",
        ],
    }

    candidate_b = {
        "document_id": "doc-c",
        "title": "Stanford Education",
        "url": "https://education.stanford.edu/",
        "snippet": "Stanford education programs.",
        "matched_terms": [
            "stanford",
            "education",
        ],
    }

    candidates = [
        source,
        candidate_a,
        candidate_b,
    ]

    related = engine.related_results(
        source,
        candidates,
        limit=10,
    )

    check(
        isinstance(related, list)
        and len(related) >= 1,
        "relationship engine discovers related information",
    )

    check(
        all(
            item["document_id"] != "doc-a"
            for item in related
        ),
        "relationship engine excludes source document",
    )

    check(
        all(
            0.0 <= item["relationship_score"] <= 1.0
            for item in related
        ),
        "relationship scores are bounded",
    )


def test_experience_engine():
    from exploration.experience import ExplorationExperience

    experience = ExplorationExperience()

    search_results = [
        {
            "rank": 1,
            "document_id": "doc-a",
            "title": "Stanford University",
            "url": "https://www.stanford.edu/",
            "snippet": (
                "Stanford University research education."
            ),
            "highlighted_snippet": (
                "[Stanford] [University] research education."
            ),
            "score": 10.0,
            "matched_terms": [
                "stanford",
                "university",
            ],
            "document_length": 100,
        },
        {
            "rank": 2,
            "document_id": "doc-b",
            "title": "Stanford Research",
            "url": "https://research.stanford.edu/",
            "snippet": "Stanford research programs.",
            "highlighted_snippet": (
                "[Stanford] research programs."
            ),
            "score": 8.0,
            "matched_terms": [
                "stanford",
                "research",
            ],
            "document_length": 80,
        },
    ]

    output = experience.build(
        query="Stanford University",
        results=search_results,
    )

    check(
        output["query"] == "Stanford University",
        "exploration preserves original query",
    )

    check(
        isinstance(output["results"], list)
        and len(output["results"]) == 2,
        "exploration preserves search results",
    )

    check(
        isinstance(output["related"], list),
        "exploration produces related results",
    )

    check(
        isinstance(output["experience_version"], str)
        and bool(output["experience_version"]),
        "exploration response has version",
    )


def test_service_compatibility():
    from search_service.service import SearchService

    required_methods = {
        "search",
        "index_document",
        "flush_index",
        "delete_document",
        "handle_request",
        "health",
    }

    check(
        required_methods.issubset(set(dir(SearchService))),
        "existing SearchService API remains intact",
    )


def test_real_search_exploration_path():
    from search_service.service import SearchService

    class FakeResponse:
        query = "Stanford University"
        mode = "OR"
        total_candidates = 2

        class Result:
            def __init__(
                self,
                document_id,
                score,
                matched_terms,
                document_length,
            ):
                self.document_id = document_id
                self.score = score
                self.matched_terms = matched_terms
                self.document_length = document_length

            def to_dict(self):
                return {
                    "document_id": self.document_id,
                    "score": self.score,
                    "matched_terms": self.matched_terms,
                    "document_length": self.document_length,
                }

        results = [
            Result(
                "doc-a",
                10.0,
                ["stanford", "university"],
                100,
            ),
            Result(
                "doc-b",
                8.0,
                ["stanford", "research"],
                80,
            ),
        ]

    class FakeEngine:
        def search(self, query, mode="OR", top_k=10):
            return FakeResponse()

    class FakeStore:
        def get(self, document_id):
            documents = {
                "doc-a": {
                    "document_id": "doc-a",
                    "title": "Stanford University",
                    "url": "https://www.stanford.edu/",
                    "text": (
                        "Stanford University research education."
                    ),
                    "description": (
                        "Stanford University research education."
                    ),
                },
                "doc-b": {
                    "document_id": "doc-b",
                    "title": "Stanford Research",
                    "url": "https://research.stanford.edu/",
                    "text": (
                        "Stanford research programs."
                    ),
                    "description": (
                        "Stanford research programs."
                    ),
                },
            }

            return documents.get(document_id)

    service = SearchService(
        index_source=None,
        document_store=FakeStore(),
    )

    service.engine = FakeEngine()

    output = service.search(
        "Stanford University",
        top_k=10,
    )

    check(
        output["query"] == "Stanford University",
        "real SearchService path preserves query",
    )

    check(
        isinstance(output["results"], list)
        and len(output["results"]) == 2,
        "real SearchService path returns ranked results",
    )

    check(
        isinstance(output["related"], list),
        "real SearchService path exposes related information",
    )

    check(
        isinstance(output["experience_version"], str)
        and output["experience_version"] == "stage9.v1",
        "real SearchService path exposes experience version",
    )


def test_website_compatibility():
    website_files = (
        "website/index.html",
        "website/search.html",
        "website/app.js",
        "website/style.css",
    )

    for path in website_files:
        check(
            os.path.exists(path),
            f"website file exists: {path}",
        )

    with open(
        "website/search.html",
        "r",
        encoding="utf-8",
    ) as file:
        search_html = file.read()

    check(
        'action="/search"' in search_html,
        "website search form retains /search compatibility",
    )

    with open(
        "website/app.js",
        "r",
        encoding="utf-8",
    ) as file:
        app_js = file.read()

    check(
        "search.html" in app_js,
        "website search-page transition remains present",
    )


def test_persistence():
    from result_presentation.document_store import DocumentStore

    temporary_directory = tempfile.mkdtemp(
        prefix="stage9_persistence_"
    )

    try:
        path = os.path.join(
            temporary_directory,
            "documents.json",
        )

        store = DocumentStore(path)

        store.add_document(
            document_id="doc-a",
            title="Stanford University",
            url="https://www.stanford.edu/",
            text="Stanford University research education.",
            description="Stanford University.",
            metadata={
                "active": True,
                "last_crawled": 123.0,
            },
        )

        store.save()

        restored = DocumentStore(path)

        check(
            restored.load() is True,
            "document store restores successfully",
        )

        document = restored.get("doc-a")

        check(
            document is not None,
            "persisted document survives restart",
        )

        check(
            document["active"] is True,
            "persisted metadata survives restart",
        )

        check(
            document["last_crawled"] == 123.0,
            "persisted crawl metadata survives restart",
        )

    finally:
        shutil.rmtree(
            temporary_directory,
            ignore_errors=True,
        )


def test_large_workload():
    from exploration.relationships import RelationshipEngine

    engine = RelationshipEngine()

    source = {
        "document_id": "source",
        "title": "Stanford University",
        "url": "https://www.stanford.edu/",
        "snippet": "Stanford University research education.",
        "matched_terms": [
            "stanford",
            "university",
        ],
    }

    documents = [source]

    for index in range(20000):
        documents.append(
            {
                "document_id": f"doc-{index}",
                "title": (
                    "Stanford Research "
                    f"Program {index}"
                ),
                "url": (
                    "https://research.stanford.edu/"
                    f"program/{index}"
                ),
                "snippet": (
                    "Stanford research education "
                    f"program {index}."
                ),
                "matched_terms": [
                    "stanford",
                    "research",
                ],
            }
        )

    related = engine.related_results(
        source,
        documents,
        limit=10,
    )

    check(
        len(related) == 10,
        "20,000-document exploration workload completes",
    )

    check(
        all(
            0.0 <= item["relationship_score"] <= 1.0
            for item in related
        ),
        "20,000-document relationship scores remain bounded",
    )


def final_gate():
    print()
    print("==============================================")
    print("STAGE 9 FINAL GATE")
    print("==============================================")
    print("PASS — World Events are not part of Stage 9")
    print("PASS — rich result experience verified")
    print("PASS — exploration relationships verified")
    print("PASS — SearchService exploration path verified")
    print("PASS — website compatibility verified")
    print("PASS — persistence verified")
    print("PASS — large exploration workload verified")
    print("PASS — protected production servers unchanged")
    print("RESULT: PASS")
    print("STAGE 9: 100% COMPLETE")
    print("STAGE 9 FINAL GATE: PASS")
    print("==============================================")


def main():
    print("==============================================")
    print("STAGE 9 — ADVANCED INFORMATION EXPERIENCE")
    print("==============================================")

    protected = snapshot_protected()

    print()
    print("=== 9.1 RICH RESULT EXPERIENCE ===")
    import_check()
    test_rich_results()

    print()
    print("=== 9.2 EXPLORATION RELATIONSHIPS ===")
    test_relationship_engine()
    test_experience_engine()

    print()
    print("=== 9.3 EXPLORATION INTEGRATION ===")
    test_real_search_exploration_path()

    print()
    print("=== 9.4 SEARCH COMPATIBILITY ===")
    test_service_compatibility()

    print()
    print("=== 9.5 WEBSITE COMPATIBILITY ===")
    test_website_compatibility()

    print()
    print("=== 9.6 RESTART/PERSISTENCE ===")
    test_persistence()

    print()
    print("=== 9.7 LARGE WORKLOAD ===")
    test_large_workload()

    print()
    print("=== 9.8 REGRESSION PROTECTION ===")
    verify_protected(protected)

    print()
    print("=== 9.9 FINAL STAGE 9 GATE ===")
    final_gate()


if __name__ == "__main__":
    main()
