import os
import sqlite3
import tempfile

from crawler_system.domain_discovery_sources import DomainCandidate
from crawler_system.domain_candidate_provenance import (
    DomainCandidateProvenance,
)
from crawler_system.domain_candidate_store import (
    DomainCandidateStore,
)


def check(name, condition):
    if not condition:
        raise AssertionError(name)
    print(f"{name}: PASS")


with tempfile.TemporaryDirectory() as directory:
    database_path = os.path.join(
        directory,
        "domain_candidates.db",
    )

    store = DomainCandidateStore(database_path)

    # ---------------------------------------------------------
    # 1. Priority is automatically calculated and persisted
    # ---------------------------------------------------------

    high = DomainCandidate(
        hostname="example.com",
        url="https://example.com/",
        source="certificate_transparency",
        evidence="ct-record",
        discovered_at=100.0,
    )

    low = DomainCandidate(
        hostname="example.org",
        url="http://example.org/path?q=test",
        source="unknown_source",
        evidence=None,
        discovered_at=101.0,
    )

    check(
        "HIGH PRIORITY INSERT",
        store.add(high),
    )

    check(
        "LOW PRIORITY INSERT",
        store.add(low),
    )

    high_record = store.get("example.com")
    low_record = store.get("example.org")

    check(
        "HIGH PRIORITY STORED",
        high_record["priority"] > 0,
    )

    check(
        "HIGH PRIORITY ABOVE LOW",
        high_record["priority"]
        > low_record["priority"],
    )


    # ---------------------------------------------------------
    # 2. Priority survives reopening the database
    # ---------------------------------------------------------

    saved_priority = high_record["priority"]

    reopened = DomainCandidateStore(database_path)

    persisted = reopened.get("example.com")

    check(
        "PRIORITY PERSISTED",
        persisted["priority"] == saved_priority,
    )


    # ---------------------------------------------------------
    # 3. Priority ordering
    # ---------------------------------------------------------

    candidates = reopened.list_candidates(
        status="discovered",
        limit=10,
    )

    check(
        "PRIORITY ORDERING",
        candidates[0]["hostname"] == "example.com",
    )

    check(
        "ORDERED HIGHER FIRST",
        candidates[0]["priority"]
        >= candidates[1]["priority"],
    )


    # ---------------------------------------------------------
    # 4. Manual priority update
    # ---------------------------------------------------------

    check(
        "PRIORITY UPDATE",
        reopened.update_priority(
            "example.org",
            99.0,
        ),
    )

    updated = reopened.get("example.org")

    check(
        "UPDATED PRIORITY STORED",
        updated["priority"] == 99.0,
    )

    reordered = reopened.list_candidates(
        status="discovered",
        limit=10,
    )

    check(
        "UPDATED PRIORITY REORDERS",
        reordered[0]["hostname"] == "example.org",
    )


    # ---------------------------------------------------------
    # 5. Invalid priority updates rejected
    # ---------------------------------------------------------

    check(
        "INVALID PRIORITY TEXT REJECTED",
        not reopened.update_priority(
            "example.org",
            "invalid",
        ),
    )

    check(
        "NEGATIVE PRIORITY REJECTED",
        not reopened.update_priority(
            "example.org",
            -1,
        ),
    )

    check(
        "OVER 100 PRIORITY REJECTED",
        not reopened.update_priority(
            "example.org",
            101,
        ),
    )


    # ---------------------------------------------------------
    # 6. Provenance insertion receives priority
    # ---------------------------------------------------------

    provenance = DomainCandidateProvenance(
        source="registry",
        evidence="registry-record",
        discovered_at=200.0,
        metadata={
            "registry": "test",
        },
    )

    check(
        "PROVENANCE INSERT WITH PRIORITY",
        reopened.add_provenance(
            "second.example",
            "https://second.example/",
            provenance,
            priority=77.5,
        ),
    )

    second = reopened.get("second.example")

    check(
        "PROVENANCE PRIORITY STORED",
        second["priority"] == 77.5,
    )

    check(
        "PROVENANCE SOURCE STORED",
        second["source"] == "registry",
    )


    # ---------------------------------------------------------
    # 7. Schema migration compatibility
    # ---------------------------------------------------------

    legacy_path = os.path.join(
        directory,
        "legacy.db",
    )

    connection = sqlite3.connect(legacy_path)

    try:
        connection.execute(
            """
            CREATE TABLE domain_candidates (
                hostname TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                source TEXT NOT NULL,
                evidence TEXT,
                discovered_at REAL NOT NULL,
                metadata_json TEXT,
                status TEXT NOT NULL DEFAULT 'discovered'
            )
            """
        )

        connection.execute(
            """
            INSERT INTO domain_candidates
            (
                hostname,
                url,
                source,
                evidence,
                discovered_at,
                metadata_json,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy.example",
                "https://legacy.example/",
                "legacy_source",
                "legacy-evidence",
                300.0,
                None,
                "discovered",
            ),
        )

        connection.commit()

    finally:
        connection.close()

    migrated = DomainCandidateStore(legacy_path)

    legacy = migrated.get("legacy.example")

    check(
        "LEGACY RECORD SURVIVES",
        legacy is not None,
    )

    check(
        "LEGACY PRIORITY COLUMN ADDED",
        "priority" in legacy,
    )

    check(
        "LEGACY DEFAULT PRIORITY",
        legacy["priority"] == 50.0,
    )

    new_legacy_candidate = DomainCandidate(
        hostname="new.example",
        url="https://new.example/",
        source="certificate_transparency",
        evidence="ct-record",
    )

    check(
        "POST-MIGRATION INSERT",
        migrated.add(new_legacy_candidate),
    )

    check(
        "POST-MIGRATION PRIORITY",
        migrated.get("new.example")["priority"] > 50.0,
    )


    # ---------------------------------------------------------
    # 8. Clear
    # ---------------------------------------------------------

    reopened.clear()

    check(
        "CLEAR PRESERVES STORE FUNCTION",
        reopened.count() == 0,
    )


print("RESULT: PASS")
