import socket
import hmac
import json
import os
import queue
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from index_storage.upload_routes import make_upload_manager
from index_storage.upload_routes import (
    read_upload_json,
    validate_capability,
    expected_chunk_length,
)
from index_storage.resumable_uploads import UploadError
from index_storage.upload_auth import UploadAuthorizationError

from index_storage.local import LocalIndexStorage
from index_storage.reverse import ReverseIndexStorage
from index_storage.repository import IndexStorageRepository
from indexing_pipeline.pipeline import CrawlIndexPipeline
from search_service.service import SearchService


STORAGE_ROOT = os.environ.get(
    "OUR_SEARCH_STORAGE_ROOT",
    "our_search_storage_data",
)

HOST = os.environ.get(
    "OUR_SEARCH_STORAGE_HOST",
    "0.0.0.0",
)

PORT = int(
    os.environ.get(
        "PORT",
        os.environ.get(
            "OUR_SEARCH_STORAGE_PORT",
            "9090",
        ),
    )
)

STORAGE_API_KEY = os.environ.get(
    "OUR_SEARCH_STORAGE_API_KEY",
)

UPLOAD_ORIGIN = os.environ.get(
    "OUR_SEARCH_UPLOAD_ORIGIN",
    "https://my-platform-20.onrender.com",
)

STORAGE_MODE = os.environ.get(
    "OUR_SEARCH_STORAGE_MODE", "local"
).strip().lower()


if not STORAGE_API_KEY:
    raise RuntimeError(
        "OUR_SEARCH_STORAGE_API_KEY is required"
    )


if STORAGE_MODE == "reverse":

    storage = ReverseIndexStorage(
        command_url=os.environ.get(
            "OUR_SEARCH_STORAGE_REVERSE_URL",
            "http://127.0.0.1:9090/reverse-command",
        ),
        api_key=STORAGE_API_KEY,
        timeout=int(
            os.environ.get(
                "OUR_SEARCH_STORAGE_TIMEOUT",
                "35",
            )
        ),
    )

else:

    storage = LocalIndexStorage(
        STORAGE_ROOT
    )

upload_manager = (
    make_upload_manager(STORAGE_ROOT)
    if STORAGE_MODE == "local"
    else None
)

REVERSE_COMMANDS = queue.Queue()

# The reverse protocol has one shared response queue.
# Only one /reverse-command request may be active at a time.
REVERSE_COMMAND_LOCK = threading.Lock()

# Search execution is only initialized when this process owns
# the local index. Reverse mode is a storage-only service and
# must not load the remote index during startup.
storage_repository = IndexStorageRepository(storage)

if STORAGE_MODE == "reverse":
    search_pipeline = None
    search_service = None
else:
    search_pipeline = CrawlIndexPipeline(
        root=STORAGE_ROOT,
        storage=storage,
        storage_repository=storage_repository,
    )
    search_service = SearchService(
        search_pipeline.search_index,
        search_pipeline.document_store,
        indexing_pipeline=search_pipeline,
    )

REVERSE_RESPONSES = queue.Queue()
REVERSE_PHONE_CONNECTED = threading.Event()


class StorageHTTPHandler(BaseHTTPRequestHandler):

    protocol_version = "HTTP/1.1"

    server_version = "OurSearchStorage/1.1"

    def _authorized(self):

        provided = self.headers.get(
            "X-Storage-API-Key",
            "",
        )

        return hmac.compare_digest(
            provided,
            STORAGE_API_KEY,
        )

    def _require_auth(self):

        if self._authorized():
            return True

        self._send_json(
            401,
            {
                "error": "unauthorized",
            },
        )

        return False

    def _add_upload_cors_headers(self):
        origin = self.headers.get("Origin", "")
        if origin == UPLOAD_ORIGIN:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
            self.send_header(
                "Access-Control-Allow-Headers",
                "Authorization, Content-Type, X-Storage-API-Key",
            )
            self.send_header(
                "Access-Control-Allow-Methods",
                "GET, POST, PUT, OPTIONS",
            )
            self.send_header("Access-Control-Max-Age", "600")

    def _validate_upload_capability(self, upload_id):
        if upload_manager is None:
            raise UploadError("resumable uploads require local storage mode")

        payload = validate_capability(self, upload_manager, upload_id)
        directory = upload_manager._directory(upload_id)
        metadata = upload_manager._read_metadata(directory)

        if (
            metadata.get("user_id") != payload.get("uid")
            or metadata.get("storage_key") != payload.get("key")
            or metadata.get("size") != payload.get("size")
            or metadata.get("upload_id") != upload_id
        ):
            raise UploadAuthorizationError(
                "Upload capability does not match its session"
            )

        return payload, metadata

    def _upload_error(self, exc):
        if isinstance(exc, UploadAuthorizationError):
            status = 401
        elif isinstance(exc, FileExistsError):
            status = 409
        elif isinstance(exc, (UploadError, ValueError, TypeError)):
            status = 400
        else:
            status = 500

        message = str(exc) if status != 500 else "upload_internal_error"
        self._send_json(status, {"error": message})

    def _send_json(
        self,
        status,
        payload,
    ):

        data = json.dumps(
            payload,
            ensure_ascii=False,
        ).encode("utf-8")

        self.send_response(status)

        self.send_header(
            "Content-Type",
            "application/json; charset=utf-8",
        )

        self.send_header(
            "Content-Length",
            str(len(data)),
        )

        self._add_upload_cors_headers()
        self.end_headers()

        self.wfile.write(data)

    def _send_bytes(
        self,
        status,
        data,
    ):

        self.send_response(status)

        self.send_header(
            "Content-Type",
            "application/octet-stream",
        )

        self.send_header(
            "Content-Length",
            str(len(data)),
        )

        self._add_upload_cors_headers()
        self.end_headers()

        self.wfile.write(data)

    def _read_body(self):

        length = int(
            self.headers.get(
                "Content-Length",
                "0",
            )
        )

        return self.rfile.read(
            length
        )

    def do_OPTIONS(self):
        if not self.path.startswith("/uploads/"):
            self._send_json(404, {"error": "not_found"})
            return

        if self.headers.get("Origin", "") != UPLOAD_ORIGIN:
            self._send_json(403, {"error": "origin_not_allowed"})
            return

        self.send_response(204)
        self._add_upload_cors_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _get_upload_status(self, upload_id):
        if STORAGE_MODE != "local" or upload_manager is None:
            self._send_json(503, {"error": "resumable_uploads_require_local_mode"})
            return

        try:
            payload, metadata = self._validate_upload_capability(upload_id)
            status = upload_manager.status(upload_id, payload["uid"])
            self._send_json(200, status)
        except Exception as exc:
            self._upload_error(exc)

    def do_GET(self):
        upload_match = __import__("re").fullmatch(
            r"/uploads/([a-f0-9]{32})",
            urlparse(self.path).path,
        )
        if upload_match:
            self._get_upload_status(upload_match.group(1))
            return


        if self.path == "/health":

            self._send_json(
                200,
                {
                    "status": "ok",
                    "service": "our_search_storage",
                },
            )

            return

        if self.path == "/reverse-poll":
            if not self._require_auth():
                return

            REVERSE_PHONE_CONNECTED.set()

            try:
                command = REVERSE_COMMANDS.get(
                    timeout=35
                )
            except queue.Empty:
                self._send_json(
                    204,
                    {},
                )
                return

            self._send_json(
                200,
                {
                    "command": command,
                },
            )
            return

        if not self._require_auth():
            return

        parsed = urlparse(
            self.path
        )

        query = parse_qs(
            parsed.query
        )

        if parsed.path == "/search":

            q = query.get(
                "q",
                [""],
            )[0].strip()

            if not q:
                self._send_json(
                    400,
                    {
                        "error": "missing query parameter: q",
                    },
                )
                return

            mode = query.get(
                "mode",
                ["OR"],
            )[0]

            try:
                top_k = int(
                    query.get(
                        "top_k",
                        ["10"],
                    )[0]
                )
            except ValueError:
                self._send_json(
                    400,
                    {
                        "error": "top_k must be an integer",
                    },
                )
                return

            top_k = max(
                1,
                min(top_k, 100),
            )

            try:
                results = search_service.search(
                    q,
                    mode=mode,
                    top_k=top_k,
                )

                self._send_json(
                    200,
                    {
                        "query": q,
                        "mode": mode,
                        "results": results,
                    },
                )

            except Exception as exc:
                self._send_json(
                    500,
                    {
                        "error": "search_failed",
                        "message": str(exc),
                    },
                )

            return

        if parsed.path == "/exists":

            key = query.get(
                "key",
                [""],
            )[0]

            self._send_json(
                200,
                {
                    "key": key,
                    "exists": storage.exists(key),
                },
            )

            return

        if parsed.path == "/get":

            key = query.get(
                "key",
                [""],
            )[0]

            data = storage.get(key)

            if data is None:

                self._send_json(
                    404,
                    {
                        "error": "not_found",
                        "key": key,
                    },
                )

                return

            self._send_bytes(
                200,
                data,
            )

            return

        if parsed.path == "/list":

            prefix = query.get(
                "prefix",
                [""],
            )[0]

            self._send_json(
                200,
                {
                    "keys": storage.list_keys(
                        prefix
                    ),
                },
            )

            return

        self._send_json(
            404,
            {
                "error": "not_found",
            },
        )

    def _start_model_upload(self):
        if STORAGE_MODE != "local" or upload_manager is None:
            self._send_json(503, {"error": "resumable_uploads_require_local_mode"})
            return

        if not self._require_auth():
            return

        try:
            data = read_upload_json(self)
            user_id = data.get("user_id")
            storage_key = data.get("storage_key")
            filename = data.get("filename")
            size = data.get("size")

            if not isinstance(size, int) or isinstance(size, bool):
                raise UploadError("Upload size must be an integer")

            session = upload_manager.start(
                filename=filename,
                size=size,
                user_id=user_id,
                storage_key=storage_key,
            )
            self._send_json(201, session)
        except Exception as exc:
            self._upload_error(exc)

    def _complete_model_upload(self, upload_id):
        if STORAGE_MODE != "local" or upload_manager is None:
            self._send_json(503, {"error": "resumable_uploads_require_local_mode"})
            return

        try:
            payload, metadata = self._validate_upload_capability(upload_id)

            verified = upload_manager.verify(
                upload_id,
                payload["uid"],
            )

            size = verified["size"]
            chunk_size = upload_manager.chunk_size
            chunk_count = (size + chunk_size - 1) // chunk_size
            directory = upload_manager._directory(upload_id)

            chunk_paths = [
                os.path.join(directory, f"{index:08d}.part")
                for index in range(chunk_count)
            ]

            manifest = storage.publish_staged_upload(
                verified["storage_key"],
                chunk_paths,
                size,
                verified["sha256"],
            )

            try:
                upload_manager.discard(upload_id, payload["uid"])
            except OSError:
                pass

            self._send_json(200, {
                "success": True,
                "upload_id": upload_id,
                "storage_key": verified["storage_key"],
                "size": size,
                "sha256": verified["sha256"],
                "published": True,
            })
        except Exception as exc:
            self._upload_error(exc)

    def do_POST(self):
        upload_match = __import__("re").fullmatch(
            r"/uploads/([a-f0-9]{32})/complete",
            urlparse(self.path).path,
        )
        if upload_match:
            self._complete_model_upload(upload_match.group(1))
            return

        if self.path == "/uploads/start":
            self._start_model_upload()
            return


        if self.path == "/reverse-command":
            if not self._require_auth():
                return

            length = int(
                self.headers.get("Content-Length", "0")
            )

            command = self.rfile.read(length).decode("utf-8").strip()

            if not command:
                self._send_json(
                    400,
                    {"error": "command_required"},
                )
                return

            if not command.startswith("REQUEST "):
                self._send_json(
                    400,
                    {"error": "request_id_required"},
                )
                return

            parts = command.split(" ", 2)

            if len(parts) != 3:
                self._send_json(
                    400,
                    {"error": "invalid_request_id_format"},
                )
                return

            _, request_id, actual_command = parts

            if not REVERSE_PHONE_CONNECTED.is_set():
                self._send_json(
                    503,
                    {"error": "phone_not_connected"},
                )
                return

            with REVERSE_COMMAND_LOCK:
                REVERSE_COMMANDS.put(command)

                while True:
                    response_id, result = REVERSE_RESPONSES.get()

                    if response_id != request_id:
                        continue

                    # Defensive normalization: the phone tunnel may return
                    # the request envelope even though /reverse-response
                    # normally strips it before queueing.
                    if result.startswith("REQUEST "):
                        response_parts = result.split(" ", 2)
                        if (
                            len(response_parts) == 3
                            and response_parts[1] == request_id
                        ):
                            result = response_parts[2]

                    self._send_json(
                        200,
                        {
                            "command": command,
                            "result": result,
                        },
                    )
                    return

        if self.path == "/reverse-response":
            if not self._require_auth():
                return

            result = self._read_body().decode("utf-8").strip()

            if not result.startswith("REQUEST "):
                self._send_json(
                    400,
                    {"error": "request_id_required"},
                )
                return

            parts = result.split(" ", 2)

            if len(parts) != 3:
                self._send_json(
                    400,
                    {"error": "invalid_response_id_format"},
                )
                return

            _, request_id, response = parts

            REVERSE_RESPONSES.put(
                (request_id, response)
            )

            self._send_json(
                200,
                {"received": True},
            )
            return

        self._send_json(
            404,
            {"error": "not_found"},
        )

    def _put_upload_chunk(self, upload_id, index):
        if STORAGE_MODE != "local" or upload_manager is None:
            self._send_json(503, {"error": "resumable_uploads_require_local_mode"})
            return

        try:
            payload, metadata = self._validate_upload_capability(upload_id)
            expected = expected_chunk_length(
                payload, index, upload_manager.chunk_size
            )

            try:
                length = int(self.headers.get("Content-Length", "-1"))
            except (TypeError, ValueError):
                raise UploadError("invalid_content_length")

            if length != expected:
                self.close_connection = True
                raise UploadError(
                    f"Incorrect chunk Content-Length: expected {expected}"
                )

            data = self.rfile.read(length)
            if len(data) != expected:
                self.close_connection = True
                raise UploadError("chunk_body_incomplete")

            result = upload_manager.write_chunk(
                upload_id,
                payload["uid"],
                index,
                data,
            )
            self._send_json(200, result)
        except Exception as exc:
            self._upload_error(exc)

    def do_PUT(self):
        upload_match = __import__("re").fullmatch(
            r"/uploads/([a-f0-9]{32})/chunks/(0|[1-9][0-9]*)",
            urlparse(self.path).path,
        )
        if upload_match:
            self._put_upload_chunk(
                upload_match.group(1),
                int(upload_match.group(2)),
            )
            return


        if self.path != "/put":

            self._send_json(
                404,
                {
                    "error": "not_found",
                },
            )

            return

        if not self._require_auth():
            return

        key = self.headers.get(
            "X-Storage-Key",
            "",
        )

        if not key:

            self._send_json(
                400,
                {
                    "error": "storage_key_required",
                },
            )

            return

        data = self._read_body()

        storage.put(
            key,
            data,
        )

        self._send_json(
            200,
            {
                "stored": True,
                "key": key,
                "size": len(data),
            },
        )

    def do_DELETE(self):

        if self.path != "/delete":

            self._send_json(
                404,
                {
                    "error": "not_found",
                },
            )

            return

        if not self._require_auth():
            return

        key = self.headers.get(
            "X-Storage-Key",
            "",
        )

        if not key:

            self._send_json(
                400,
                {
                    "error": "storage_key_required",
                },
            )

            return

        deleted = storage.delete(
            key
        )

        self._send_json(
            200,
            {
                "deleted": deleted,
                "key": key,
            },
        )

    def log_message(
        self,
        format,
        *args,
    ):

        print(
            "%s - %s"
            % (
                self.address_string(),
                format % args,
            )
        )


def main():

    server = ThreadingHTTPServer(
        (
            HOST,
            PORT,
        ),
        StorageHTTPHandler,
    )

    print(
        "OUR SEARCH STORAGE SERVER"
    )

    print(
        f"Running on {HOST}:{PORT}"
    )

    print(
        f"Storage root: {STORAGE_ROOT}"
    )

    print(
        "API key authentication: enabled"
    )

    print(
        "Press Ctrl+C to stop."
    )

    try:

        server.serve_forever()

    except KeyboardInterrupt:

        print(
            "\nStopping storage server..."
        )

    finally:

        server.server_close()


if __name__ == "__main__":
    main()
