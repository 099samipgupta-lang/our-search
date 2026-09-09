import html
import os
import json
import urllib.parse
import urllib.request
import urllib.error
import traceback
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


SEARCH_API = "https://my-platform-11.onrender.com/search"


def log(message):
    print(message, flush=True)


class WebsiteHandler(SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/search":
            self.handle_search()
            return

        try:
            super().do_GET()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def handle_search(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        query = params.get("q", [""])[0].strip()

        if not query:
            self.send_response(302)
            self.send_header("Location", "/")
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            return

        request_data = json.dumps({
            "query": query,
            "mode": "OR",
            "top_k": 10
        }).encode("utf-8")

        log(
            f"[SEARCH] Starting API request: "
            f"query={query!r}"
        )

        try:
            request = urllib.request.Request(
                SEARCH_API,
                data=request_data,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                method="POST",
            )

            log(
                f"[SEARCH] Sending request to: "
                f"{SEARCH_API}"
            )

            with urllib.request.urlopen(
                request,
                timeout=30
            ) as response:

                response_body = response.read()

                log(
                    f"[SEARCH] API response: "
                    f"status={response.status}, "
                    f"reason={response.reason}"
                )

                log(
                    f"[SEARCH] API response headers: "
                    f"{dict(response.headers)}"
                )

                log(
                    f"[SEARCH] API response body bytes: "
                    f"{len(response_body)}"
                )

                data = json.loads(
                    response_body.decode("utf-8")
                )

            results = data.get("results", [])

            log(
                f"[SEARCH] API returned "
                f"{len(results)} results"
            )

            page = self.render_results(
                query,
                results
            )

            body = page.encode("utf-8")

            self.send_response(200)
            self.send_header(
                "Content-Type",
                "text/html; charset=utf-8"
            )
            self.send_header(
                "Content-Length",
                str(len(body))
            )
            self.send_header(
                "Connection",
                "close"
            )
            self.end_headers()

            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        except urllib.error.HTTPError as error:
            log(
                f"[SEARCH] API HTTP ERROR: "
                f"status={error.code}, "
                f"reason={error.reason}"
            )

            try:
                log(
                    f"[SEARCH] API HTTP ERROR headers: "
                    f"{dict(error.headers)}"
                )
            except Exception:
                log(
                    "[SEARCH] Could not read HTTP error headers"
                )

            try:
                error_body = error.read().decode(
                    "utf-8",
                    errors="replace"
                )

                log(
                    "[SEARCH] API HTTP ERROR body: "
                    f"{error_body[:2000]}"
                )
            except Exception as body_error:
                log(
                    "[SEARCH] Could not read HTTP error body: "
                    f"{body_error}"
                )

            log("[SEARCH] API HTTP ERROR traceback:")
            traceback.print_exc()

            self.send_search_error(
                f"HTTP Error {error.code}: {error.reason}"
            )

        except Exception as error:
            log(
                "[SEARCH] Unexpected search error: "
                f"type={type(error).__name__}, "
                f"message={error}"
            )

            log("[SEARCH] Unexpected error traceback:")
            traceback.print_exc()

            self.send_search_error(
                str(error)
            )

    def send_search_error(self, message):
        safe_message = html.escape(message)

        body = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport"
          content="width=device-width, initial-scale=1.0">

    <title>Search Error — Our Search</title>

    <link rel="stylesheet" href="/style.css">
</head>

<body class="search-page">

    <header class="search-header">

        <a href="/" class="search-logo"
           aria-label="Our Search home">
            Our Search
        </a>

        <form
            class="search-page-form"
            method="GET"
            action="/search"
        >

            <span
                class="search-page-icon"
                aria-hidden="true"
            >⌕</span>

            <input
                id="search-input"
                type="search"
                name="q"
                placeholder="Search anything..."
                autocomplete="off"
                value=""
                autofocus
            >

            <button
                type="submit"
                aria-label="Search"
            >→</button>

        </form>

    </header>

    <main class="search-main">

        <div class="search-intro">

            <h1>Search error</h1>

            <p>
                {safe_message}
            </p>

        </div>

    </main>

</body>
</html>
"""

        encoded = body.encode("utf-8")

        self.send_response(500)
        self.send_header(
            "Content-Type",
            "text/html; charset=utf-8"
        )
        self.send_header(
            "Content-Length",
            str(len(encoded))
        )
        self.send_header(
            "Connection",
            "close"
        )
        self.end_headers()

        try:
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def render_results(self, query, results):
        result_items = []

        for result in results:
            title = html.escape(
                str(
                    result.get(
                        "title",
                        "Untitled result"
                    )
                )
            )

            url = str(
                result.get(
                    "url",
                    result.get(
                        "source_url",
                        "#"
                    )
                )
            )

            safe_url = html.escape(
                url,
                quote=True
            )

            description = html.escape(
                str(
                    result.get(
                        "description",
                        result.get(
                            "snippet",
                            ""
                        )
                    )
                )
            )

            result_items.append(
                f"""
                <article class="search-result">

                    <a
                        class="search-result-title"
                        href="{safe_url}"
                        target="_blank"
                        rel="noopener noreferrer"
                    >
                        {title}
                    </a>

                    <div class="search-result-url">
                        {safe_url}
                    </div>

                    <p class="search-result-description">
                        {description}
                    </p>

                </article>
                """
            )

        results_html = "\n".join(result_items)

        if not results_html:
            results_html = """
            <div class="search-no-results">
                <h2>No results found</h2>
                <p>Our Search could not find matching results.</p>
            </div>
            """

        return f"""<!DOCTYPE html>
<html lang="en">

<head>

    <meta charset="UTF-8">

    <meta
        name="viewport"
        content="width=device-width, initial-scale=1.0"
    >

    <title>
        {html.escape(query)} — Our Search
    </title>

    <link
        rel="stylesheet"
        href="/style.css"
    >

</head>

<body class="search-page">

    <header class="search-header">

        <a
            href="/"
            class="search-logo"
            aria-label="Our Search home"
        >
            Our Search
        </a>

        <form
            class="search-page-form"
            method="GET"
            action="/search"
        >

            <span
                class="search-page-icon"
                aria-hidden="true"
            >⌕</span>

            <input
                id="search-input"
                type="search"
                name="q"
                value="{html.escape(query)}"
                placeholder="Search anything..."
                autocomplete="off"
                enterkeyhint="search"
            >

            <button
                type="submit"
                aria-label="Search"
            >
                →
            </button>

        </form>

    </header>


    <main class="search-main">

        <div class="search-results">

            <div class="search-results-heading">

                <h1>
                    Search results
                </h1>

                <p>
                    Results for
                    <strong>
                        {html.escape(query)}
                    </strong>
                </p>

            </div>

            {results_html}

        </div>

    </main>


    <script>

        window.addEventListener(
            "pageshow",
            function () {{

                const input =
                    document.getElementById(
                        "search-input"
                    );

                if (input) {{

                    input.focus();

                    const length =
                        input.value.length;

                    input.setSelectionRange(
                        length,
                        length
                    );

                }}

            }}
        );

    </script>

</body>
</html>
"""


if __name__ == "__main__":

    website_directory = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "website"
    )

    os.chdir(website_directory)

    port = int(
        os.environ.get(
            "PORT",
            "3000"
        )
    )

    server = ThreadingHTTPServer(
        ("0.0.0.0", port),
        WebsiteHandler
    )

    log(
        f"Our Search website server "
        f"running on port {port}"
    )

    try:
        server.serve_forever()

    except KeyboardInterrupt:
        log("Server stopped.")

    finally:
        server.server_close()
