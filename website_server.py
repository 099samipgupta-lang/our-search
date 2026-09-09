import html
import json
import os
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


SEARCH_API = "https://my-platform-11.onrender.com/search"

API_TIMEOUT = 30
MAX_RETRIES = 1
MAX_RETRY_DELAY = 5


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
            self.send_redirect_home()
            return

        log(
            f"[SEARCH] Starting search: "
            f"query={query!r}"
        )

        try:
            data = self.request_search_api(query)

            results = data.get("results", [])

            if not isinstance(results, list):
                log(
                    "[SEARCH] API returned an invalid "
                    "'results' value"
                )
                results = []

            log(
                f"[SEARCH] API returned "
                f"{len(results)} results"
            )

            page = self.render_results(
                query,
                results
            )

            self.send_html_response(
                200,
                page
            )

        except urllib.error.HTTPError as error:
            log(
                f"[SEARCH] Final upstream HTTP error: "
                f"status={error.code}, "
                f"reason={error.reason}"
            )

            self.log_http_error(error)

            if error.code == 429:
                self.send_search_error(
                    "Search service is temporarily busy. "
                    "Please try again in a moment.",
                    status_code=503
                )
            else:
                self.send_search_error(
                    f"Search service returned "
                    f"HTTP {error.code}: {error.reason}",
                    status_code=502
                )

        except urllib.error.URLError as error:
            log(
                "[SEARCH] Upstream connection error: "
                f"type={type(error).__name__}, "
                f"reason={error.reason}"
            )

            log("[SEARCH] Upstream connection traceback:")
            traceback.print_exc()

            self.send_search_error(
                "Could not connect to the search service.",
                status_code=503
            )

        except TimeoutError as error:
            log(
                "[SEARCH] Search request timed out: "
                f"{error}"
            )

            log("[SEARCH] Timeout traceback:")
            traceback.print_exc()

            self.send_search_error(
                "The search service took too long to respond.",
                status_code=504
            )

        except json.JSONDecodeError as error:
            log(
                "[SEARCH] Invalid JSON received from API: "
                f"{error}"
            )

            log("[SEARCH] JSON traceback:")
            traceback.print_exc()

            self.send_search_error(
                "The search service returned invalid data.",
                status_code=502
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
                "An unexpected search error occurred.",
                status_code=500
            )

    def request_search_api(self, query):
        request_data = json.dumps({
            "query": query,
            "mode": "OR",
            "top_k": 10
        }).encode("utf-8")

        for attempt in range(MAX_RETRIES + 1):
            log(
                f"[SEARCH] API attempt "
                f"{attempt + 1}/{MAX_RETRIES + 1}: "
                f"{SEARCH_API}"
            )

            request = urllib.request.Request(
                SEARCH_API,
                data=request_data,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "User-Agent": "OurSearchWebsite/1.0",
                    "Connection": "close",
                },
                method="POST",
            )

            try:
                with urllib.request.urlopen(
                    request,
                    timeout=API_TIMEOUT
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

                    if response.status != 200:
                        raise RuntimeError(
                            f"Unexpected API status "
                            f"{response.status}"
                        )

                    return json.loads(
                        response_body.decode("utf-8")
                    )

            except urllib.error.HTTPError as error:

                if error.code != 429:
                    raise

                log(
                    f"[SEARCH] API returned 429 "
                    f"on attempt {attempt + 1}"
                )

                retry_after = self.get_retry_after(
                    error
                )

                self.log_http_error(error)

                if attempt >= MAX_RETRIES:
                    log(
                        "[SEARCH] 429 retry limit reached. "
                        "No more attempts."
                    )
                    raise

                delay = retry_after

                if delay is None:
                    delay = 1

                delay = max(
                    0,
                    min(delay, MAX_RETRY_DELAY)
                )

                log(
                    f"[SEARCH] Waiting {delay} seconds "
                    f"before one controlled retry"
                )

                time.sleep(delay)

        raise RuntimeError(
            "Search API request failed"
        )

    def get_retry_after(self, error):
        try:
            value = error.headers.get("Retry-After")

            if value is None:
                log(
                    "[SEARCH] No Retry-After header "
                    "was provided"
                )
                return None

            value = value.strip()

            log(
                f"[SEARCH] Retry-After header: "
                f"{value!r}"
            )

            return int(value)

        except (ValueError, TypeError):
            log(
                "[SEARCH] Retry-After was not a "
                "numeric delay"
            )
            return None

    def log_http_error(self, error):
        try:
            log(
                f"[SEARCH] Upstream HTTP error headers: "
                f"{dict(error.headers)}"
            )
        except Exception:
            log(
                "[SEARCH] Could not read "
                "upstream error headers"
            )

        try:
            error_body = error.read().decode(
                "utf-8",
                errors="replace"
            )

            log(
                "[SEARCH] Upstream HTTP error body: "
                f"{error_body[:2000]}"
            )

        except Exception as body_error:
            log(
                "[SEARCH] Could not read "
                f"upstream error body: {body_error}"
            )

    def send_redirect_home(self):
        try:
            self.send_response(302)
            self.send_header(
                "Location",
                "/"
            )
            self.send_header(
                "Connection",
                "close"
            )
            self.end_headers()

        except (BrokenPipeError, ConnectionResetError):
            pass

        finally:
            self.close_connection = True

    def send_html_response(self, status_code, page):
        body = page.encode("utf-8")

        try:
            self.send_response(status_code)

            self.send_header(
                "Content-Type",
                "text/html; charset=utf-8"
            )

            self.send_header(
                "Content-Length",
                str(len(body))
            )

            self.send_header(
                "Cache-Control",
                "no-store"
            )

            self.send_header(
                "Connection",
                "close"
            )

            self.end_headers()

            self.wfile.write(body)

        except (BrokenPipeError, ConnectionResetError):
            pass

        finally:
            self.close_connection = True

    def send_search_error(
        self,
        message,
        status_code=500
    ):
        safe_message = html.escape(
            str(message)
        )

        body = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport"
          content="width=device-width, initial-scale=1.0">

    <title>Search Error — Our Search</title>

    <link rel="stylesheet"
          href="/style.css">
</head>

<body class="search-page">

    <header class="search-header">

        <a href="/"
           class="search-logo"
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

        self.send_html_response(
            status_code,
            body
        )

    def render_results(self, query, results):
        safe_query = html.escape(
            query
        )

        result_cards = []

        for result in results:
            if not isinstance(result, dict):
                continue

            title = str(
                result.get("title")
                or result.get("name")
                or "Untitled result"
            )

            url = str(
                result.get("url")
                or result.get("link")
                or ""
            )

            description = str(
                result.get("description")
                or result.get("snippet")
                or ""
            )

            if not url:
                continue

            safe_title = html.escape(
                title
            )

            safe_url = html.escape(
                url,
                quote=True
            )

            safe_description = html.escape(
                description
            )

            result_cards.append(
                f"""
                <article class="search-result">

                    <a
                        class="search-result-title"
                        href="{safe_url}"
                        target="_blank"
                        rel="noopener noreferrer"
                    >
                        {safe_title}
                    </a>

                    <div class="search-result-url">
                        {safe_url}
                    </div>

                    <p class="search-result-description">
                        {safe_description}
                    </p>

                </article>
                """
            )

        if result_cards:
            results_html = "\n".join(
                result_cards
            )
        else:
            results_html = """
                <div class="search-no-results">
                    <h2>No results found</h2>
                    <p>
                        Our Search could not find
                        matching results.
                    </p>
                </div>
            """

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">

    <meta
        name="viewport"
        content="width=device-width,
                 initial-scale=1.0"
    >

    <title>
        {safe_query} — Our Search
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
                placeholder="Search anything..."
                autocomplete="off"
                value="{safe_query}"
                enterkeyhint="search"
            >

            <button
                type="submit"
                aria-label="Search"
            >→</button>

        </form>

    </header>

    <main class="search-main">

        <div class="search-results-header">

            <h1>
                Search results
            </h1>

            <p>
                Results for
                <strong>
                    {safe_query}
                </strong>
            </p>

        </div>

        <section class="search-results">

            {results_html}

        </section>

    </main>

</body>
</html>
"""


if __name__ == "__main__":
    website_directory = os.path.join(
        os.path.dirname(
            os.path.abspath(__file__)
        ),
        "website"
    )

    if not os.path.isdir(
        website_directory
    ):
        raise RuntimeError(
            "Website directory not found: "
            f"{website_directory}"
        )

    os.chdir(
        website_directory
    )

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
        "[SERVER] Our Search website server "
        f"starting on port {port}"
    )

    log(
        "[SERVER] Website directory: "
        f"{website_directory}"
    )

    log(
        "[SERVER] Search API: "
        f"{SEARCH_API}"
    )

    try:
        server.serve_forever()

    except KeyboardInterrupt:
        log(
            "[SERVER] Shutdown requested"
        )

    finally:
        server.server_close()

        log(
            "[SERVER] Server stopped"
        )
