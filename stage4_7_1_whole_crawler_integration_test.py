import tempfile
import time

from crawler_system.whole_web_crawler import WholeWebCrawler


def main():
    with tempfile.TemporaryDirectory() as tmp:
        crawler = WholeWebCrawler(
            worker_count=1,
            frontier_delay=0,
            task_timeout=10,
            max_attempts=1,
            storage_root=tmp,
        )

        try:
            controller = crawler.expansion_controller

            assert controller is not None
            print("CONTROLLER ATTACHED: PASS")

            candidates = [
                {
                    "hostname": f"continuous-{i}.example",
                    "first_url": f"https://continuous-{i}.example/",
                    "source_hostname": "source.example",
                    "discovered_at": time.time(),
                }
                for i in range(5)
            ]

            for i, candidate in enumerate(candidates):
                crawler.expansion_queue.enqueue(
                    candidate,
                    priority=100 - i,
                )

            assert crawler.expansion_queue.count("queued") == 5
            print("CANDIDATES QUEUED: PASS")

            consumed = []

            original_add_url = crawler._add_url

            def fake_add_url(
                url,
                source="unknown",
                depth=0,
                seed=False,
                source_url=None,
            ):
                consumed.append(
                    {
                        "url": url,
                        "source": source,
                        "depth": depth,
                        "seed": seed,
                        "source_url": source_url,
                    }
                )
                return True

            crawler._add_url = fake_add_url

            first_cycle = controller.run_cycle()

            assert first_cycle["error"] is None
            assert first_cycle["processed"] == 5
            assert len(consumed) == 5
            print("CONTROLLER PROCESSES EXPANSION QUEUE: PASS")

            assert crawler.expansion_queue.count("queued") == 0
            assert crawler.expansion_queue.count("processing") == 0
            assert crawler.expansion_queue.count("complete") == 5
            print("QUEUE FULLY PROCESSED: PASS")

            assert all(
                item["source"] == "expansion"
                for item in consumed
            )
            print("EXPANSION SOURCE PROPAGATION: PASS")

            assert len(
                {item["url"] for item in consumed}
            ) == 5
            print("NO DUPLICATE PROCESSING: PASS")

            status = crawler.status()

            assert "expansion_controller" in status
            assert status["expansion_controller"]["complete"] == 5
            print("CRAWLER STATUS INCLUDES CONTROLLER: PASS")

            assert status["expansion_controller"]["stats"]["processed"] == 5
            print("CONTROLLER METRICS PROPAGATED: PASS")

            # Verify expired lease recovery through the integrated controller.
            recovery_candidate = {
                "hostname": "recovery.example",
                "first_url": "https://recovery.example/",
                "source_hostname": "source.example",
                "discovered_at": time.time(),
            }

            crawler.expansion_queue.enqueue(
                recovery_candidate,
                priority=90,
            )

            claimed = crawler.expansion_queue.claim_next()

            assert claimed is not None
            print("LEASE CANDIDATE CLAIMED: PASS")

            lease_connection = crawler.expansion_queue._connect()
            try:
                lease_connection.execute(
                    """
                    UPDATE expansion_queue
                    SET processing_started_at = ?
                    WHERE hostname = ?
                    """,
                    (time.time() - 120, "recovery.example"),
                )
                lease_connection.commit()
            finally:
                lease_connection.close()

            # The queue implementation owns lease recovery. The controller
            # must invoke it before processing the next expansion batch.
            recovery_cycle = controller.run_cycle()

            assert recovery_cycle["error"] is None
            assert recovery_cycle["recovered"] >= 1
            print("EXPIRED LEASE RECOVERED BY CONTROLLER: PASS")

            assert crawler.expansion_queue.count("complete") == 6
            print("RECOVERED CANDIDATE COMPLETED: PASS")

            assert crawler.expansion_queue.count("processing") == 0
            print("NO STUCK PROCESSING LEASES: PASS")

            crawler._add_url = original_add_url

            print("RESULT: PASS")

        finally:
            crawler.stop()


if __name__ == "__main__":
    main()
