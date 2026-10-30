import base64
import html
import json
import os
import traceback
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

from index_storage.repository import IndexStorageRepository
from index_storage.config import create_index_storage
from indexing_pipeline.pipeline import CrawlIndexPipeline
from search_service.service import SearchService


# ------------------------------------------------------------
# Repository paths
# ------------------------------------------------------------

REPOSITORY_ROOT = os.path.dirname(
    os.path.abspath(__file__)
)

WEBSITE_DIRECTORY = os.path.join(
    REPOSITORY_ROOT,
    "website",
)

INDEX_ROOT = os.path.join(
    REPOSITORY_ROOT,
    "indexing_pipeline_data",
)


# ------------------------------------------------------------
# Supabase configuration
# ------------------------------------------------------------

# ------------------------------------------------------------
# Storage backend
# ------------------------------------------------------------

storage = create_index_storage()

storage_repository = IndexStorageRepository(
    storage
)

indexing_pipeline = CrawlIndexPipeline(
    root=INDEX_ROOT,
    storage=storage,
    storage_repository=storage_repository,
)

search_index = indexing_pipeline.search_index
document_store = indexing_pipeline.document_store

search_service = SearchService(
    search_index,
    document_store,
    indexing_pipeline=indexing_pipeline,
)


# ------------------------------------------------------------
# HTTP handler
# ------------------------------------------------------------

class WebsiteHandler(SimpleHTTPRequestHandler):

    protocol_version = "HTTP/1.0"

    def __init__(
        self,
        *args,
        **kwargs
    ):
        super().__init__(
            *args,
            directory=WEBSITE_DIRECTORY,
            **kwargs
        )

    # --------------------------------------------------------
    # JSON response
    # --------------------------------------------------------

    def send_json(
        self,
        status_code,
        payload
    ):
        body = json.dumps(
            payload,
            ensure_ascii=False,
        ).encode("utf-8")

        try:
            self.send_response(
                status_code
            )

            self.send_header(
                "Content-Type",
                "application/json; charset=utf-8",
            )

            self.send_header(
                "Content-Length",
                str(len(body)),
            )

            self.send_header(
                "Cache-Control",
                "no-store",
            )

            self.send_header(
                "Access-Control-Allow-Origin",
                "*",
            )

            self.send_header(
                "Access-Control-Allow-Methods",
                "GET, POST, OPTIONS",
            )

            self.send_header(
                "Access-Control-Allow-Headers",
                "Content-Type",
            )

            self.send_header(
                "Connection",
                "close",
            )

            self.end_headers()

            self.wfile.write(body)
            self.wfile.flush()

        except (
            BrokenPipeError,
            ConnectionResetError,
            ConnectionAbortedError,
        ):
            return

    # --------------------------------------------------------
    # HTML response
    # --------------------------------------------------------

    def send_html(
        self,
        status_code,
        body
    ):
        encoded = body.encode(
            "utf-8"
        )

        try:
            self.send_response(
                status_code
            )

            self.send_header(
                "Content-Type",
                "text/html; charset=utf-8",
            )

            self.send_header(
                "Content-Length",
                str(len(encoded)),
            )

            self.send_header(
                "Cache-Control",
                "no-store",
            )

            self.send_header(
                "Connection",
                "close",
            )

            self.end_headers()

            self.wfile.write(encoded)
            self.wfile.flush()

        except (
            BrokenPipeError,
            ConnectionResetError,
            ConnectionAbortedError,
        ):
            return

    # --------------------------------------------------------
    # OPTIONS
    # --------------------------------------------------------

    def do_OPTIONS(self):

        try:
            self.send_response(
                204
            )

            self.send_header(
                "Access-Control-Allow-Origin",
                "*",
            )

            self.send_header(
                "Access-Control-Allow-Methods",
                "GET, POST, OPTIONS",
            )

            self.send_header(
                "Access-Control-Allow-Headers",
                "Content-Type",
            )

            self.send_header(
                "Connection",
                "close",
            )

            self.end_headers()

        except (
            BrokenPipeError,
            ConnectionResetError,
            ConnectionAbortedError,
        ):
            return

    # --------------------------------------------------------
    # GET
    # --------------------------------------------------------

    def do_GET(self):

        try:

            parsed = urlparse(
                self.path
            )

            path = parsed.path

            # ----------------------------------------------
            # Health
            # ----------------------------------------------

            if path == "/health":

                self.send_json(
                    200,
                    search_service.health(),
                )

                return

            # ----------------------------------------------
            # Website search
            # ----------------------------------------------

            if path == "/search":

                self.handle_search(
                    parsed.query
                )

                return

            # ----------------------------------------------
            # Everything else = static website
            # ----------------------------------------------

            super().do_GET()

        except (
            BrokenPipeError,
            ConnectionResetError,
            ConnectionAbortedError,
        ):
            return

        except Exception as error:

            traceback.print_exc()

            self.send_html(
                500,
                self.render_error_page(
                    "Internal server error",
                    str(error),
                ),
            )

    # --------------------------------------------------------
    # POST
    # --------------------------------------------------------


    def handle_model_upload(self):
        import http.client
        import re
        from urllib.request import Request, urlopen

        api_key = os.environ.get("OUR_SEARCH_STORAGE_API_KEY", "")
        if not api_key:
            self.send_json(500, {"error": "storage_not_configured"})
            return

        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            self.send_json(401, {"error": "sign_in_required"})
            return

        try:
            request = Request(
                "https://rxhyxmfayalrjmicsomm.supabase.co/auth/v1/user",
                headers={
                    "apikey": "sb_publishable_3bKiIeuQq_yVrNK59Bk96A_9Y31q8hI",
                    "Authorization": auth,
                },
            )
            with urlopen(request, timeout=15) as response:
                user = json.loads(response.read().decode("utf-8"))
        except Exception:
            self.send_json(401, {"error": "session_invalid"})
            return

        user_id = str(user.get("id", ""))
        storage_key = self.headers.get("X-Model-Storage-Key", "")
        if (
            not user_id
            or not storage_key.startswith("ai-models/" + user_id + "/")
            or ".." in storage_key
            or not re.fullmatch(r"[A-Za-z0-9._/-]+", storage_key)
        ):
            self.send_json(400, {"error": "invalid_storage_key"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0:
            self.send_json(400, {"error": "empty_upload"})
            return

        connection = None
        try:
            connection = http.client.HTTPSConnection(
                "my-platform-9.onrender.com", timeout=600
            )
            connection.putrequest("PUT", "/put")
            connection.putheader("Content-Type", "application/octet-stream")
            connection.putheader("Content-Length", str(length))
            connection.putheader("X-Storage-API-Key", api_key)
            connection.putheader("X-Storage-Key", storage_key)
            connection.endheaders()

            remaining = length
            while remaining:
                chunk = self.rfile.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise ConnectionError("Upload ended before all bytes arrived")
                connection.send(chunk)
                remaining -= len(chunk)

            result = connection.getresponse()
            response_body = result.read(4096)
            if result.status < 200 or result.status >= 300:
                self.send_json(
                    502,
                    {
                        "error": "storage_upload_failed",
                        "storage_status": result.status,
                    },
                )
                return

            self.send_json(
                200,
                {"stored": True, "key": storage_key, "size": length},
            )
        except Exception:
            self.send_json(502, {"error": "storage_connection_failed"})
        finally:
            if connection:
                connection.close()

    def handle_model_upload_start(self):
        import http.client
        import re
        from urllib.request import Request, urlopen
        from index_storage.upload_auth import issue_upload_token

        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            self.send_json(401, {"error": "sign_in_required"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 65536:
                self.send_json(400, {"error": "invalid_request_size"})
                return
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:
            self.send_json(400, {"error": "invalid_request"})
            return

        try:
            request = Request(
                "https://rxhyxmfayalrjmicsomm.supabase.co/auth/v1/user",
                headers={
                    "apikey": "sb_publishable_3bKiIeuQq_yVrNK59Bk96A_9Y31q8hI",
                    "Authorization": auth,
                },
            )
            with urlopen(request, timeout=15) as response:
                user = json.loads(response.read().decode("utf-8"))
        except Exception:
            self.send_json(401, {"error": "session_invalid"})
            return

        user_id = str(user.get("id", ""))
        filename = data.get("filename")
        size = data.get("size")
        storage_key = data.get("storage_key")

        if (
            not user_id
            or not isinstance(filename, str)
            or not filename.strip()
            or not isinstance(size, int)
            or isinstance(size, bool)
            or size <= 0
            or size > 20 * 1024 * 1024 * 1024
            or not isinstance(storage_key, str)
            or not storage_key.startswith("ai-models/" + user_id + "/")
            or ".." in storage_key
            or not re.fullmatch(r"[A-Za-z0-9._/-]+", storage_key)
        ):
            self.send_json(400, {"error": "invalid_upload_details"})
            return

        api_key = os.environ.get("OUR_SEARCH_STORAGE_API_KEY", "")
        if not api_key:
            self.send_json(500, {"error": "storage_not_configured"})
            return

        connection = None
        try:
            import json as json_module

            connection = http.client.HTTPSConnection(
                "my-platform-9.onrender.com", timeout=60
            )
            body = json_module.dumps({
                "filename": filename,
                "size": size,
                "user_id": user_id,
                "storage_key": storage_key,
            })
            connection.request(
                "POST",
                "/uploads/start",
                body=body,
                headers={
                    "Content-Type": "application/json",
                    "X-Storage-API-Key": api_key,
                    "Content-Length": str(len(body.encode("utf-8"))),
                },
            )
            response = connection.getresponse()
            raw = response.read(65536)
            if response.status != 201:
                self.send_json(502, {
                    "error": "storage_session_start_failed",
                    "storage_status": response.status,
                })
                return

            session = json.loads(raw.decode("utf-8"))
            upload_id = session["upload_id"]
            token = issue_upload_token(
                user_id=user_id,
                storage_key=storage_key,
                size=size,
                upload_id=upload_id,
                ttl=21600,
            )
            self.send_json(201, {
                "upload_id": upload_id,
                "chunk_size": session["chunk_size"],
                "chunks_received": session["chunks_received"],
                "upload_token": token,
                "upload_url": "https://my-platform-9.onrender.com",
                "token_ttl_seconds": 21600,
            })
        except Exception:
            traceback.print_exc()
            self.send_json(502, {"error": "upload_session_creation_failed"})
        finally:
            if connection:
                connection.close()

    def do_POST(self):

        parsed = urlparse(
            self.path
        )

        path = parsed.path

        if path == "/model-upload/start":
            self.handle_model_upload_start()
            return

        if path == "/model-upload":
            self.handle_model_upload()
            return

        if path == "/ai/chat":
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > 100_000:
                    raise ValueError("Invalid request size")

                payload = json.loads(self.rfile.read(length).decode("utf-8"))
                messages = payload.get("messages", [])

                if not isinstance(messages, list) or not messages:
                    raise ValueError("Chat messages are required")

                prompt_parts = []
                for message in messages[-20:]:
                    if not isinstance(message, dict):
                        continue
                    role = str(message.get("role", "user"))
                    content = str(message.get("text", message.get("content", "")))
                    if content:
                        prompt_parts.append(f"{role.capitalize()}: {content}")

                prompt = "\\n".join(prompt_parts) + "\\nAssistant:"
                if not prompt_parts:
                    raise ValueError("No usable chat messages")

                max_tokens = max(1, min(int(payload.get("max_tokens", 128)), 256))
                encoded = base64.b64encode(prompt.encode("utf-8")).decode("ascii")

                result = storage._command(f"AI {encoded} {max_tokens}")

                if not result.startswith("AI_RESULT:"):
                    self.send_json(502, {"error": result or "Qwen returned no response"})
                    return

                answer = base64.b64decode(
                    result[len("AI_RESULT:"):],
                    validate=True
                ).decode("utf-8")

                self.send_json(200, {"response": answer})
            except Exception as error:
                self.send_json(502, {"error": str(error)})
            return

        allowed_paths = (
            "/search",
            "/index",
            "/delete",
            "/flush",
        )

        if path not in allowed_paths:

            self.send_json(
                404,
                {
                    "error": "not_found"
                },
            )

            return

        try:

            # ----------------------------------------------
            # Flush does not require a request body
            # ----------------------------------------------

            if path == "/flush":

                result = (
                    search_service.flush_index()
                )

                self.send_json(
                    200,
                    result,
                )

                return

            # ----------------------------------------------
            # Read request body
            # ----------------------------------------------

            content_length = int(
                self.headers.get(
                    "Content-Length",
                    "0",
                )
            )

            if content_length <= 0:

                raise ValueError(
                    "request body is empty"
                )

            if content_length > 1_000_000:

                raise ValueError(
                    "request body is too large"
                )

            body = self.rfile.read(
                content_length
            )

            payload = json.loads(
                body.decode(
                    "utf-8"
                )
            )

            # ----------------------------------------------
            # Search
            # ----------------------------------------------

            if path == "/search":

                result = (
                    search_service.handle_request(
                        payload
                    )
                )

            # ----------------------------------------------
            # Index
            # ----------------------------------------------

            elif path == "/index":

                result = (
                    search_service.index_document(
                        payload
                    )
                )

            # ----------------------------------------------
            # Delete
            # ----------------------------------------------

            elif path == "/delete":

                result = (
                    search_service.delete_document(
                        payload
                    )
                )

            else:

                raise ValueError(
                    "unsupported request"
                )

            self.send_json(
                200,
                result,
            )

        except json.JSONDecodeError:

            self.send_json(
                400,
                {
                    "error": "invalid_json"
                },
            )

        except ValueError as error:

            self.send_json(
                400,
                {
                    "error": str(error)
                },
            )

        except (
            BrokenPipeError,
            ConnectionResetError,
            ConnectionAbortedError,
        ):
            return

        except Exception as error:

            traceback.print_exc()

            self.send_json(
                500,
                {
                    "error": "internal_server_error",
                    "message": str(error),
                },
            )

    # --------------------------------------------------------
    # Website search
    # --------------------------------------------------------

    def handle_search(
        self,
        query_string
    ):

        params = parse_qs(
            query_string
        )

        query_values = params.get(
            "q",
            [],
        )

        query = (
            query_values[0]
            if query_values
            else ""
        )

        query = query.strip()

        # ----------------------------------------------
        # Empty query
        # ----------------------------------------------

        if not query:

            self.send_redirect_home()

            return

        print(
            f"[SEARCH] Query: {query}",
            flush=True,
        )

        try:

            result = search_service.search(
                query,
                mode="OR",
                top_k=10,
            )


            if isinstance(result.get("results"), dict):
                result["results"] = result["results"].get(
                    "results",
                    [],
                )

            self.send_html(
                200,
                self.render_results(
                    query,
                    result,
                ),
            )

        except Exception as error:

            traceback.print_exc()

            self.send_html(
                500,
                self.render_error_page(
                    "Search error",
                    str(error),
                ),
            )

    # --------------------------------------------------------
    # Redirect
    # --------------------------------------------------------

    def send_redirect_home(self):

        try:

            self.send_response(
                302
            )

            self.send_header(
                "Location",
                "/",
            )

            self.send_header(
                "Cache-Control",
                "no-store",
            )

            self.send_header(
                "Connection",
                "close",
            )

            self.end_headers()

        except (
            BrokenPipeError,
            ConnectionResetError,
            ConnectionAbortedError,
        ):
            return

    # --------------------------------------------------------
    # Search result page
    # --------------------------------------------------------

    def render_results(
        self,
        query,
        response,
    ):

        safe_query = html.escape(
            query
        )

        results = response.get(
            "results",
            []
        )

        result_cards = []

        for result in results:

            if not isinstance(
                result,
                dict
            ):
                continue

            title = result.get(
                "title",
                ""
            )

            url = result.get(
                "url",
                ""
            )

            description = result.get(
                "description",
                ""
            )

            if not isinstance(
                title,
                str
            ):
                title = str(title)

            if not isinstance(
                url,
                str
            ):
                url = str(url)

            if not isinstance(
                description,
                str
            ):
                description = str(
                    description
                )

            safe_title = html.escape(
                title
            )

            safe_url = html.escape(
                url,
                quote=True,
            )

            safe_description = (
                html.escape(
                    description
                )
            )

            if not safe_title:

                safe_title = safe_url

            if not safe_url:
                continue

            result_cards.append(
                f"""
                <article class="search-result">
                    <a
                        class="search-result-link"
                        href="{safe_url}"
                        target="_blank"
                        rel="noopener noreferrer"
                    >
                        <h2 class="search-result-title">
                            {safe_title}
                        </h2>

                        <div class="search-result-url">
                            {safe_url}
                        </div>

                        <p class="search-result-description">
                            {safe_description}
                        </p>
                    </a>
                </article>
                """
            )

        if result_cards:

            results_html = "\n".join(
                result_cards
            )

        else:

            results_html = """
                <div class="no-results">
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
        content="width=device-width, initial-scale=1.0"
    >

    <title>
        Search results — Our Search
    </title>

    <link
        rel="stylesheet"
        href="/style.css"
    >
    <link
        rel="stylesheet"
        href="/experience.css"
    >
</head>

<body class="search-page">

    <header class="search-header">

        <a
            class="search-logo"
            href="/"
        >
            OUR SEARCH
        </a>

    </header>

    <main class="search-results-page">

        <form
            class="search-page-form"
            method="GET"
            action="/search"
        >

            <input
                id="search-input"
                name="q"
                type="search"
                value="{safe_query}"
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

        <section class="search-results">

            <div class="search-results-heading">
                <p>
                    Results for
                    <strong>{safe_query}</strong>
                </p>
            </div>

            {self.render_brain_experience(response)}

            {results_html}

        </section>

    </main>

</body>
</html>
"""

    # --------------------------------------------------------
    # OUR SEARCH Brain Experience
    # --------------------------------------------------------


    def render_brain_experience(self, result):
        brain = result.get("brain", {})

        if not isinstance(brain, dict) or not brain:
            return ""

        understanding = brain.get("understanding", {})
        knowledge = brain.get("knowledge", {})
        reasoning = brain.get("reasoning", {})
        verification = brain.get("verification", {})
        conversation = brain.get("conversation", {})
        concepts = brain.get("concepts", [])

        topic = html.escape(
            str(
                understanding.get(
                    "normalized_query",
                    result.get("query", ""),
                )
            )
        )

        generated_answer = conversation.get(
            "answer",
            "",
        )

        safe_answer = html.escape(
            str(generated_answer)
        )

        answer_evidence = conversation.get(
            "evidence",
            [],
        )

        evidence_items = []

        for evidence in answer_evidence:
            if not isinstance(evidence, dict):
                continue

            title = html.escape(
                str(evidence.get("title", "Source"))
            )

            url = html.escape(
                str(evidence.get("url", ""))
            )

            snippet = html.escape(
                str(evidence.get("snippet", ""))
            )

            if url:
                evidence_items.append(
                    '<div class="our-brain-source">'
                    f'<a href="{url}" target="_blank" rel="noopener">'
                    f'{title}'
                    '</a>'
                    f'<p>{snippet}</p>'
                    '</div>'
                )
            else:
                evidence_items.append(
                    '<div class="our-brain-source">'
                    f'<strong>{title}</strong>'
                    f'<p>{snippet}</p>'
                    '</div>'
                )

        evidence_html = "".join(evidence_items)

        source_count = verification.get(
            "source_count",
            0,
        )

        grounded = conversation.get(
            "grounded",
            False,
        )

        if grounded:
            status = (
                "Answer generated from retrieved search evidence."
            )
        else:
            status = (
                "Not enough retrieved evidence to construct "
                "a grounded answer."
            )

        safe_status = html.escape(status)

        concept_items = []

        for concept in concepts:
            if not isinstance(concept, dict):
                continue

            name = html.escape(
                str(concept.get("name", ""))
            )

            if name:
                concept_items.append(
                    f'<span class="our-brain-concept">{name}</span>'
                )

        concepts_html = "".join(concept_items)

        return (
            '<section class="our-brain-experience">'
            '<div class="our-brain-label">'
            'OUR SEARCH BRAIN'
            '</div>'

            '<h2 class="our-brain-title">'
            f'{topic}'
            '</h2>'

            '<section class="our-brain-section our-brain-answer">'
            '<h3>Answer</h3>'
            '<p>'
            f'{safe_answer}'
            '</p>'
            '</section>'

            '<p class="our-brain-status">'
            f'{safe_status} '
            f'Sources: {html.escape(str(source_count))}'
            '</p>'

            '<section class="our-brain-section">'
            '<h3>Understanding</h3>'
            '<p>'
            f'{html.escape(str(understanding.get("intent", "The query was analyzed by the language system.")))}'
            '</p>'
            '</section>'

            '<section class="our-brain-section">'
            '<h3>Concepts</h3>'
            '<div class="our-brain-concepts">'
            f'{concepts_html}'
            '</div>'
            '</section>'

            '<section class="our-brain-section">'
            '<h3>Evidence</h3>'
            '<div class="our-brain-sources">'
            f'{evidence_html}'
            '</div>'
            '</section>'

            '<section class="our-brain-section">'
            '<h3>Reasoning</h3>'
            '<p>'
            'The reasoning system analyzed the retrieved '
            'evidence and relationships before answer '
            'construction.'
            '</p>'
            '</section>'

            '</section>'
        )

    # --------------------------------------------------------
    # Error page
    # --------------------------------------------------------

    def render_error_page(
        self,
        title,
        message,
    ):

        safe_title = html.escape(
            title
        )

        safe_message = html.escape(
            message
        )

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">

    <meta
        name="viewport"
        content="width=device-width, initial-scale=1.0"
    >

    <title>
        {safe_title} — Our Search
    </title>

    <link
        rel="stylesheet"
        href="/style.css"
    >
</head>

<body class="search-page">

    <header class="search-header">

        <a
            class="search-logo"
            href="/"
        >
            OUR SEARCH
        </a>

    </header>

    <main class="search-results-page">

        <section class="no-results">

            <h1>
                {safe_title}
            </h1>

            <p>
                {safe_message}
            </p>

            <p>
                <a href="/">
                    Return to Our Search
                </a>
            </p>

        </section>

    </main>

</body>
</html>
"""

    # --------------------------------------------------------
    # Logging
    # --------------------------------------------------------

    def log_message(
        self,
        format_string,
        *args
    ):

        print(
            "[HTTP] "
            + format_string % args,
            flush=True,
        )


# ------------------------------------------------------------
# Server
# ------------------------------------------------------------

class WebsiteSearchServer(
    ThreadingHTTPServer
):

    allow_reuse_address = True
    daemon_threads = True


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

server = None

try:

    if not os.path.isdir(
        WEBSITE_DIRECTORY
    ):

        raise RuntimeError(
            "Website directory not found: "
            + WEBSITE_DIRECTORY
        )

    HOST = "0.0.0.0"

    PORT = int(
        os.environ.get(
            "PORT",
            "3000",
        )
    )

    server = WebsiteSearchServer(
        (
            HOST,
            PORT,
        ),
        WebsiteHandler,
    )

    print(
        "OUR SEARCH WEBSITE + SEARCH ENGINE",
        flush=True,
    )

    print(
        f"Running on {HOST}:{PORT}",
        flush=True,
    )

    print(
        f"Website: {WEBSITE_DIRECTORY}",
        flush=True,
    )

    print(
        f"Index root: {INDEX_ROOT}",
        flush=True,
    )

    print(
        "Storage backend: configured by OUR_SEARCH_STORAGE_MODE",
        flush=True,
    )

    print(
        "Search engine: LOCAL PROCESS",
        flush=True,
    )

    print(
        "External Search API dependency: NONE",
        flush=True,
    )

    server.serve_forever()

except KeyboardInterrupt:

    print(
        "\nStopping server...",
        flush=True,
    )

finally:

    if server is not None:

        server.server_close()

    indexing_pipeline.close()
