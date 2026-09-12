import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from crawler_system.certificate_transparency_domain_source import (
    CertificateTransparencyDomainSource,
)
from crawler_system.domain_discovery_sources import (
    DomainDiscoveryContext,
)


PAYLOAD = [
    {
        "name_value": "example.com\nwww.example.com\n*.api.example.com"
    },
    {
        "name_value": "another-example.org"
    },
    {
        "common_name": "common.example.net"
    },
    {
        "name_value": "*.INVALID_DOMAIN"
    },
    {
        "name_value": "localhost"
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
        daemon=True
    )

    thread.start()

    try:
        source = CertificateTransparencyDomainSource(
            endpoint=(
                f"http://127.0.0.1:{port}/?q={{query}}"
            )
        )

        context = DomainDiscoveryContext()

        assert source.can_discover(
            context
        ) is True

        candidates = source.discover(
            context
        )

        hostnames = {
            candidate.hostname
            for candidate in candidates
        }

        expected = {
            "example.com",
            "www.example.com",
            "api.example.com",
            "another-example.org",
            "common.example.net",
        }

        assert hostnames == expected, (
            f"Unexpected hostnames: {hostnames}"
        )

        for candidate in candidates:
            assert (
                candidate.source
                == "certificate_transparency"
            )

            assert candidate.url.startswith(
                "https://"
            )

            assert candidate.evidence.startswith(
                f"http://127.0.0.1:{port}/"
            )

            assert candidate.metadata[
                "discovery_method"
            ] == "certificate_transparency"

        print(
            "SOURCE REGISTRATION CONTRACT: PASS"
        )
        print(
            "LOCAL CT RESPONSE FETCH: PASS"
        )
        print(
            "HOSTNAME EXTRACTION: PASS"
        )
        print(
            "WILDCARD NORMALIZATION: PASS"
        )
        print(
            "INVALID CANDIDATE REJECTION: PASS"
        )
        print(
            "PROVENANCE: PASS"
        )
        print(
            "CANDIDATE OUTPUT: PASS"
        )
        print(
            "RESULT: PASS"
        )

    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
