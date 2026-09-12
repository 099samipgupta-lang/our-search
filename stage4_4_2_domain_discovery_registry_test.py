import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from crawler_system.certificate_transparency_domain_source import (
    CertificateTransparencyDomainSource,
)
from crawler_system.domain_discovery_sources import (
    DomainDiscoveryContext,
    DomainDiscoverySourceRegistry,
)


PAYLOAD = [
    {
        "name_value": (
            "registry-example.com\n"
            "*.api.registry-example.com"
        )
    },
    {
        "name_value": "registry-example.org"
    },
]


class TestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        body = json.dumps(PAYLOAD).encode("utf-8")

        self.send_response(200)
        self.send_header(
            "Content-Type",
            "application/json"
        )
        self.send_header(
            "Content-Length",
            str(len(body))
        )
        self.end_headers()

        self.wfile.write(body)

    def log_message(self, *args):
        pass


def main():

    server = HTTPServer(
        ("127.0.0.1", 0),
        TestHandler
    )

    port = server.server_address[1]

    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
    )

    thread.start()

    try:

        source = CertificateTransparencyDomainSource(
            endpoint=(
                f"http://127.0.0.1:{port}/?q={{query}}"
            )
        )

        registry = DomainDiscoverySourceRegistry()

        registry.register(source)

        assert registry.names() == (
            "certificate_transparency",
        )

        context = DomainDiscoveryContext()

        candidates = registry.discover(
            context
        )

        hostnames = {
            candidate.hostname
            for candidate in candidates
        }

        expected = {
            "registry-example.com",
            "api.registry-example.com",
            "registry-example.org",
        }

        assert hostnames == expected, (
            f"Unexpected hostnames: {hostnames}"
        )

        for candidate in candidates:
            assert (
                candidate.source
                == "certificate_transparency"
            )

            assert candidate.evidence.startswith(
                f"http://127.0.0.1:{port}/"
            )

        metrics = registry.metrics(
            "certificate_transparency"
        )

        assert metrics is not None
        assert metrics["executions"] == 1
        assert metrics["successful_executions"] == 1
        assert metrics["failures"] == 0
        assert metrics["items_produced"] == 3
        assert metrics["empty_results"] == 0
        assert metrics["health"] == "healthy"

        print(
            "SOURCE REGISTRATION: PASS"
        )
        print(
            "REGISTRY EXECUTION: PASS"
        )
        print(
            "CANDIDATE PROPAGATION: PASS"
        )
        print(
            "PROVENANCE PROPAGATION: PASS"
        )
        print(
            "SOURCE METRICS: PASS"
        )
        print(
            "REGISTRY HEALTH: PASS"
        )
        print(
            "RESULT: PASS"
        )

    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
