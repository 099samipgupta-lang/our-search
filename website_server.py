import html
import os
import json
import urllib.parse
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


SEARCH_API = "https://my-platform-11.onrender.com/search"


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

            with urllib.request.urlopen(
                request,
                timeout=30
            ) as response:
                data = json.loads(
                    response.read().decode("utf-8")
                )

            results = data.get("results", [])

            page = self.render_results(query, results)

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
                "Cache-Control",
                "no-store"
            )
            self.send_header(
                "Connection",
                "close"
            )
            self.end_headers()

            try:
                self.wfile.write(body)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass

            self.close_connection = True

        except Exception as error:

            message = html.escape(str(error))

            body = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Our Search</title>
</head>
<body>
    <h1>Our Search</h1>
    <p>Search error: {message}</p>
    <p><a href="/">Back</a></p>
</body>
</html>
""".encode("utf-8")

            self.send_response(500)
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
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass

            self.close_connection = True

    def render_results(self, query, results):

        query_html = html.escape(query)

        result_html = ""

        for item in results:

            url = html.escape(
                str(item.get("url", ""))
            )

            title = html.escape(
                str(
                    item.get("title")
                    or item.get("url")
                    or ""
                )
            )

            snippet = html.escape(
                str(item.get("snippet") or "")
            )

            result_html += f"""
<article class="result">
    <a href="{url}" rel="noopener">
        {title}
    </a>

    <div class="result-url">
        {url}
    </div>

    <div class="result-snippet">
        {snippet}
    </div>
</article>
"""

        if not results:
            result_html = """
<p>No results found.</p>
"""

        return f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport"
          content="width=device-width, initial-scale=1.0">
    <title>Our Search</title>
    <link rel="stylesheet" href="/style.css">
</head>

<body>

<main class="page">

    <section class="hero">

        <h1>Our Search</h1>

        <form
            class="search-box"
            method="GET"
            action="/search"
        >

            <input
                name="q"
                type="search"
                value="{query_html}"
                placeholder="What do you want to know?"
                autocomplete="off"
            >

            <button type="submit">
                Search
            </button>

        </form>

    </section>

    <section class="results-section">

        <div id="status">
            Search results for:
            <strong>{query_html}</strong>
        </div>

        <div id="results">
            {result_html}
        </div>

    </section>

</main>

</body>
</html>
"""

    def log_message(self, format_string, *args):
        print(format_string % args)


class WebsiteServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":

    server = WebsiteServer(
        ("0.0.0.0", int(os.environ.get("PORT", 3000))),
        lambda *args, **kwargs:
            WebsiteHandler(
                *args,
                directory="website",
                **kwargs
            ),
    )

    print("OUR SEARCH WEBSITE")
    print("Running on Render")
    print("Press Ctrl+C to stop.")

    try:
        server.serve_forever()

    except KeyboardInterrupt:
        print("\nStopping website...")

    finally:
        server.server_close()
