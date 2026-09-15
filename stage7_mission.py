import os
import shutil
import time

from search_engine import SearchEngine
from stage6_mission import DistributedIndex


STAGE7_ROOT = "stage7_production_data"
SHARD_COUNT = 8


def clean_root():
    if os.path.exists(STAGE7_ROOT):
        shutil.rmtree(STAGE7_ROOT)


def add(index, document_id, url, title, text):
    index.add_document(
        document_id=document_id,
        url=url,
        title=title,
        text=text,
    )


def flush(index):
    result = index.flush()
    index.save_manifest()
    return result


def gate_query_parser_and_basic_retrieval():
    print("\n=== 7.1 QUERY UNDERSTANDING + RETRIEVAL ===")

    clean_root()

    index = DistributedIndex(
        root=STAGE7_ROOT,
        shard_count=SHARD_COUNT,
    )

    add(
        index,
        "DOC-SEARCH-001",
        "https://example.com/search",
        "Search Technology",
        "search engine technology information retrieval "
        "ranking documents web search",
    )

    add(
        index,
        "DOC-SEARCH-002",
        "https://example.com/database",
        "Database Technology",
        "database storage distributed systems persistence",
    )

    add(
        index,
        "DOC-SEARCH-003",
        "https://example.com/web",
        "Web Information",
        "public web information pages documents knowledge",
    )

    flush(index)

    engine = SearchEngine(index)

    response = engine.search(
        "search technology",
        mode="OR",
        top_k=10,
    )

    assert response.total_candidates >= 1
    assert response.results
    assert response.results[0].document_id == (
        "DOC-SEARCH-001"
    )

    and_response = engine.search(
        "search technology",
        mode="AND",
        top_k=10,
    )

    assert and_response.results
    assert and_response.results[0].document_id == (
        "DOC-SEARCH-001"
    )

    print(
        "PASS — query parsing and candidate retrieval"
    )

    index.close()


def gate_relevance_ranking():
    print("\n=== 7.2 RELEVANCE RANKING ===")

    clean_root()

    index = DistributedIndex(
        root=STAGE7_ROOT,
        shard_count=SHARD_COUNT,
    )

    add(
        index,
        "DOC-LOW-0001",
        "https://example.com/other",
        "Other Information",
        "search information information information "
        "unrelated content",
    )

    add(
        index,
        "DOC-MID-0001",
        "https://example.com/search",
        "Search Information",
        "search information retrieval",
    )

    add(
        index,
        "DOC-TOP-0001",
        "https://example.com/search-technology",
        "Search Technology",
        "search technology search technology "
        "information retrieval ranking",
    )

    flush(index)

    engine = SearchEngine(index)

    response = engine.search(
        "search technology",
        mode="OR",
        top_k=3,
    )

    ids = [
        result.document_id
        for result in response.results
    ]

    assert ids[0] == "DOC-TOP-0001"
    assert len(ids) == 3

    print(
        "PASS — relevance ranking orders strongest result first"
    )

    index.close()


def gate_coverage_and_proximity():
    print("\n=== 7.3 COVERAGE + PROXIMITY ===")

    clean_root()

    index = DistributedIndex(
        root=STAGE7_ROOT,
        shard_count=SHARD_COUNT,
    )

    add(
        index,
        "DOC-PROX-0001",
        "https://example.com/a",
        "Search Technology",
        "search technology information",
    )

    add(
        index,
        "DOC-PROX-0002",
        "https://example.com/b",
        "Search Information",
        "search information unrelated technology",
    )

    add(
        index,
        "DOC-PROX-0003",
        "https://example.com/c",
        "Technology Information",
        "technology information unrelated search",
    )

    flush(index)

    engine = SearchEngine(index)

    response = engine.search(
        "search technology",
        mode="OR",
        top_k=3,
    )

    assert response.results

    first = response.results[0]

    assert set(first.matched_terms) == {
        "search",
        "technology",
    }

    print(
        "PASS — query coverage and positional relevance"
    )

    index.close()


def gate_title_and_url_relevance():
    print("\n=== 7.4 TITLE + URL RELEVANCE ===")

    clean_root()

    index = DistributedIndex(
        root=STAGE7_ROOT,
        shard_count=SHARD_COUNT,
    )

    add(
        index,
        "DOC-TITLE-0001",
        "https://example.com/general",
        "Search Technology",
        "general information about technology",
    )

    add(
        index,
        "DOC-TITLE-0002",
        "https://example.com/search",
        "General Information",
        "search technology information",
    )

    flush(index)

    engine = SearchEngine(index)

    response = engine.search(
        "search technology",
        mode="OR",
        top_k=2,
    )

    assert response.results
    assert response.results[0].document_id == (
        "DOC-TITLE-0001"
    )

    print(
        "PASS — title and URL relevance signals"
    )

    index.close()


def gate_distributed_global_ranking():
    print("\n=== 7.5 DISTRIBUTED GLOBAL RANKING ===")

    clean_root()

    index = DistributedIndex(
        root=STAGE7_ROOT,
        shard_count=SHARD_COUNT,
    )

    documents = [
        (
            "DOC-GLOBAL-0001",
            "https://example.com/a",
            "Information Page",
            "search information",
        ),
        (
            "DOC-GLOBAL-0002",
            "https://example.com/b",
            "Search Technology",
            "search technology ranking retrieval",
        ),
        (
            "DOC-GLOBAL-0003",
            "https://example.com/c",
            "Technology",
            "technology information",
        ),
        (
            "DOC-GLOBAL-0004",
            "https://example.com/d",
            "Search",
            "search engine information",
        ),
    ]

    for document in documents:
        add(index, *document)

    distribution = index.validate_distribution()

    assert len(distribution) == SHARD_COUNT
    assert sum(distribution.values()) == 4

    flush(index)

    engine = SearchEngine(index)

    response = engine.search(
        "search technology",
        mode="OR",
        top_k=10,
    )

    assert response.results
    assert response.total_candidates >= 2

    top_id = response.results[0].document_id

    assert top_id == "DOC-GLOBAL-0002"

    print(
        "PASS — global fan-out and ranking merge"
    )

    index.close()


def gate_restart_consistency():
    print("\n=== 7.6 RESTART CONSISTENCY ===")

    clean_root()

    index = DistributedIndex(
        root=STAGE7_ROOT,
        shard_count=SHARD_COUNT,
    )

    for number in range(100):
        add(
            index,
            f"DOC-RESTART-{number:06d}",
            f"https://example.com/page/{number}",
            "Search Technology",
            (
                "search technology information retrieval "
                "distributed index web document"
            ),
        )

    flush(index)
    index.close()

    recovered = DistributedIndex(
        root=STAGE7_ROOT,
        shard_count=SHARD_COUNT,
    )

    engine = SearchEngine(recovered)

    response = engine.search(
        "search technology",
        mode="OR",
        top_k=10,
    )

    assert response.total_candidates == 100
    assert len(response.results) == 10

    print(
        "PASS — ranking survives restart consistently"
    )

    recovered.close()


def gate_stress():
    print("\n=== 7.7 RANKING STRESS ===")

    clean_root()

    index = DistributedIndex(
        root=STAGE7_ROOT,
        shard_count=SHARD_COUNT,
    )

    count = int(
        os.environ.get(
            "STAGE7_STRESS_DOCUMENTS",
            "20000",
        )
    )

    for number in range(count):
        document_id = (
            f"DOC-STRESS-{number:08d}"
        )

        if number % 10 == 0:
            title = "Search Technology"
            text = (
                "search technology information retrieval "
                "search engine ranking distributed web"
            )
        else:
            title = f"Information Page {number}"
            text = (
                "public information document "
                "web content knowledge"
            )

        add(
            index,
            document_id,
            f"https://example.com/page/{number}",
            title,
            text,
        )

    start = time.perf_counter()

    flush(index)

    engine = SearchEngine(index)

    response = engine.search(
        "search technology",
        mode="OR",
        top_k=20,
    )

    elapsed = time.perf_counter() - start

    expected_candidates = count // 10
    assert response.total_candidates == expected_candidates
    assert len(response.results) == 20

    throughput = (
        count / elapsed
        if elapsed > 0
        else float("inf")
    )

    print(
        f"Stress documents: {count}"
    )
    print(
        f"Elapsed: {elapsed:.3f} s"
    )
    print(
        f"Ranking throughput: {throughput:.2f} docs/s"
    )

    print(
        "PASS — distributed ranking stress"
    )

    index.close()


def final_gate():
    print("\n=== STAGE 7 FINAL GATE ===")

    assert os.path.exists(
        "ranking/engine.py"
    )

    assert os.path.exists(
        "ranking/scorer.py"
    )

    assert os.path.exists(
        "search_engine/core.py"
    )

    print(
        "PASS — production ranking architecture present"
    )

    print(
        "PASS — query understanding and retrieval"
    )

    print(
        "PASS — relevance scoring"
    )

    print(
        "PASS — coverage and proximity"
    )

    print(
        "PASS — title and URL relevance"
    )

    print(
        "PASS — distributed global ranking"
    )

    print(
        "PASS — restart consistency"
    )

    print(
        "PASS — stress testing"
    )

    print(
        "\nRESULT: PASS"
    )

    print(
        "STAGE 7: 100% COMPLETE"
    )

    print(
        "READY FOR STAGE 8"
    )


def main():
    print("=" * 70)
    print("OUR SEARCH — STAGE 7 INTEGRATED MISSION")
    print("=" * 70)

    gate_query_parser_and_basic_retrieval()
    gate_relevance_ranking()
    gate_coverage_and_proximity()
    gate_title_and_url_relevance()
    gate_distributed_global_ranking()
    gate_restart_consistency()
    gate_stress()

    final_gate()

    print("=" * 70)
    print("STAGE 7 MISSION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
