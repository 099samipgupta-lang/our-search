import signal
import time

from crawler_system.whole_web_crawler import WholeWebCrawler
from crawler_system.shared_worker_pool import SharedQueueWorkerPool

STOP = False


def stop(signum, frame):
    global STOP
    STOP = True


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)

crawler = WholeWebCrawler(
    worker_count=2,
    frontier_delay=0,
    task_timeout=30,
    max_attempts=2,
    index_url="http://127.0.0.1:8082/index",
)

crawler.coordinator.pool = SharedQueueWorkerPool(
    worker_count=2
)

crawler.running = True
crawler.coordinator.start()

print("LIVE CRAWLER STARTED", flush=True)
print("INITIAL_FRONTIER:", crawler.frontier.size(), flush=True)

last_report = time.time()

try:
    while not STOP:
        crawler.coordinator.monitor()
        crawler.coordinator.dispatch()

        crawler.global_web_discovery.cycle()

        results = crawler.coordinator.collect(timeout=0.5)

        for result in results:
            if result is not None:
                crawler._process_result(result)

        if time.time() - last_report >= 10:
            s = crawler.status()
            print(
                "PROGRESS:",
                "completed=", s["stats"]["pages_completed"],
                "failed=", s["stats"]["pages_failed"],
                "discovered=", s["stats"]["discovered"],
                "accepted=", s["stats"]["accepted_urls"],
                "frontier=", s["frontier"],
                "indexed=", s["indexing"]["bridge"]["bridge"]["indexed"], "index_failed=", s["indexing"]["bridge"]["bridge"]["failed"],
                flush=True,
            )
            last_report = time.time()

        time.sleep(0.05)

finally:
    print("STOPPING", flush=True)

    try:
        crawler.index_integration.flush()
    except Exception:
        pass

    try:
        crawler.state_storage.save(
            crawler.url_dedup,
            crawler.content_dedup,
            crawler.change_tracker,
        )
    except Exception:
        pass

    crawler.coordinator.stop()
    crawler.running = False
    print("FINAL_FRONTIER:", crawler.frontier.size(), flush=True)
