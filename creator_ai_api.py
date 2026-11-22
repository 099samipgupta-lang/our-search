import hashlib
import ipaddress
import json
import os
import re
import socket
import ssl
import threading
import time
import http.client
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from creator_ai_security import (
    decrypt_api_key,
    create_test_token,
    encrypt_api_key,
    hash_test_token,
)


SUPABASE_PUBLIC_KEY = (
    "sb_publishable_3bKiIeuQq_yVrNK59Bk96A_9Y31q8hI"
)


def _authenticate(auth):
    if not auth.startswith("Bearer "):
        raise PermissionError("sign_in_required")

    request = Request(
        "https://rxhyxmfayalrjmicsomm.supabase.co/auth/v1/user",
        headers={
            "apikey": SUPABASE_PUBLIC_KEY,
            "Authorization": auth,
        },
    )

    try:
        with urlopen(request, timeout=15) as response:
            user = json.loads(response.read().decode("utf-8"))
    except Exception:
        raise PermissionError("session_invalid")

    user_id = str(user.get("id", ""))
    if not user_id:
        raise PermissionError("session_invalid")

    return user_id


def _read_payload(handler):
    try:
        length = int(handler.headers.get("Content-Length", "0"))
    except ValueError:
        raise ValueError("invalid_request_size")

    if length <= 0 or length > 20000:
        raise ValueError("invalid_request_size")

    try:
        payload = json.loads(
            handler.rfile.read(length).decode("utf-8")
        )
    except Exception:
        raise ValueError("invalid_json")

    if not isinstance(payload, dict):
        raise ValueError("invalid_request")

    return payload


def _validate_endpoint(value):
    if not isinstance(value, str) or len(value) > 2048:
        raise ValueError("invalid_endpoint")

    parsed = urlsplit(value.strip())

    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.port not in (None, 443)
    ):
        raise ValueError("endpoint_must_be_public_https")

    host = parsed.hostname.encode("idna").decode("ascii")

    if host.lower() in ("localhost",) or host.endswith(".local"):
        raise ValueError("endpoint_not_allowed")

    try:
        addresses = socket.getaddrinfo(
            host, 443, type=socket.SOCK_STREAM
        )
    except OSError:
        raise ValueError("endpoint_dns_failed")

    ips = list(dict.fromkeys(item[4][0] for item in addresses))

    if not ips:
        raise ValueError("endpoint_dns_failed")

    for address in ips:
        if not ipaddress.ip_address(address).is_global:
            raise ValueError("endpoint_not_allowed")

    path = parsed.path.rstrip("/")

    if not path.endswith("/chat/completions"):
        path += "/chat/completions"

    return host, ips[0], path or "/chat/completions"


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host, ip, timeout):
        super().__init__(
            host,
            port=443,
            timeout=timeout,
            context=ssl.create_default_context(),
        )
        self.pinned_ip = ip

    def connect(self):
        sock = socket.create_connection(
            (self.pinned_ip, 443),
            self.timeout,
        )
        self.sock = self._context.wrap_socket(
            sock,
            server_hostname=self.host,
        )


def _test_upstream(endpoint, model, auth_type, api_key, header_name):
    host, ip, path = _validate_endpoint(endpoint)

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    if auth_type == "bearer":
        headers["Authorization"] = "Bearer " + api_key

    elif auth_type == "header":
        if (
            not re.fullmatch(r"[A-Za-z0-9-]{1,64}", header_name)
            or header_name.lower() in {
                "host", "content-length", "connection",
                "transfer-encoding", "content-type",
            }
        ):
            raise ValueError("invalid_auth_header")

        headers[header_name] = api_key

    body = json.dumps({
        "model": model,
        "messages": [
            {"role": "user", "content": "Reply with OK."}
        ],
        "max_tokens": 8,
    }).encode("utf-8")

    connection = _PinnedHTTPSConnection(host, ip, 20)

    try:
        connection.request("POST", path, body=body, headers=headers)
        response = connection.getresponse()

        if response.status < 200 or response.status >= 300:
            response.read(1024)
            raise ValueError("creator_api_rejected_test")

        raw = response.read(65537)

        if len(raw) > 65536:
            raise ValueError("creator_api_response_too_large")

        result = json.loads(raw.decode("utf-8"))

        if (
            not isinstance(result, dict)
            or not isinstance(result.get("choices"), list)
            or not result["choices"]
        ):
            raise ValueError("invalid_creator_api_response")

    finally:
        connection.close()


def _supabase_request(path, payload):
    base = os.environ.get("SUPABASE_URL", "").rstrip("/")
    secret = os.environ.get("SUPABASE_SECRET_KEY", "")

    if not base or not secret:
        raise RuntimeError("database_not_configured")

    parsed = urlsplit(base)

    if parsed.scheme != "https" or not parsed.hostname:
        raise RuntimeError("database_not_configured")

    data = json.dumps(payload).encode("utf-8")

    request = Request(
        base + "/rest/v1/" + path,
        data=data,
        method="POST",
        headers={
            "apikey": secret,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )

    try:
        with urlopen(request, timeout=20) as response:
            return response.read(1000000)
    except HTTPError as error:
        error.read(1024)
        raise RuntimeError("database_operation_failed")
    except URLError:
        raise RuntimeError("database_unavailable")


def handle_test_connection(handler):
    try:
        user_id = _authenticate(
            handler.headers.get("Authorization", "")
        )
        payload = _read_payload(handler)

        provider = str(payload.get("provider", "")).strip()
        endpoint = str(payload.get("endpoint_url", "")).strip()
        model = str(payload.get("model", "")).strip()
        api_format = payload.get("api_format", "openai")
        auth_type = payload.get("auth_type", "")
        api_key = payload.get("api_key", "")
        header_name = payload.get("header_name", "")

        if (
            not provider or len(provider) > 80
            or not model or len(model) > 200
            or api_format != "openai"
            or auth_type not in ("bearer", "header", "none")
            or not isinstance(api_key, str)
            or len(api_key) > 4096
        ):
            raise ValueError("invalid_connection_details")

        if auth_type == "none":
            api_key = ""
        elif not api_key:
            raise ValueError("api_key_required")

        _test_upstream(
            endpoint, model, auth_type, api_key, header_name
        )

        encrypted_key = (
            encrypt_api_key(api_key) if api_key else ""
        )

        token, token_hash = create_test_token()

        _supabase_request(
            "ai_creator_connections",
            {
                "creator_id": user_id,
                "provider": provider,
                "endpoint_url": endpoint,
                "api_format": api_format,
                "model": model,
                "auth_type": auth_type,
                "header_name": header_name or None,
                "encrypted_api_key": encrypted_key,
                "test_token_hash": token_hash,
                "status": "staged",
                "expires_at": (
                    datetime.now(timezone.utc)
                    + timedelta(minutes=10)
                ).isoformat(),
            },
        )

        handler.send_json(
            200,
            {
                "ok": True,
                "test_token": token,
                "message": "Connection test successful.",
            },
        )

    except PermissionError as error:
        handler.send_json(401, {"error": str(error)})
    except ValueError as error:
        handler.send_json(400, {"error": str(error)})
    except Exception:
        handler.send_json(
            502, {"error": "connection_test_failed"}
        )


def handle_save_connection(handler):
    try:
        user_id = _authenticate(
            handler.headers.get("Authorization", "")
        )
        payload = _read_payload(handler)

        token = payload.get("test_token", "")
        name = str(payload.get("name", "")).strip()
        description = str(payload.get("description", "")).strip()
        category = str(payload.get("category", "")).strip()

        if (
            not isinstance(token, str)
            or len(token) > 200
            or not name or len(name) > 120
            or not description or len(description) > 3000
            or not category or len(category) > 100
        ):
            raise ValueError("invalid_submission_details")

        _supabase_request(
            "rpc/save_creator_ai_connection",
            {
                "p_test_token_hash": hash_test_token(token),
                "p_creator_id": user_id,
                "p_name": name,
                "p_description": description,
                "p_category": category,
            },
        )

        handler.send_json(
            200,
            {
                "ok": True,
                "status": "connected",
            },
        )

    except PermissionError as error:
        handler.send_json(401, {"error": str(error)})
    except ValueError as error:
        handler.send_json(400, {"error": str(error)})
    except Exception:
        handler.send_json(
            502, {"error": "connection_save_failed"}
        )


# Per-user request limit: 20 messages per minute per server process.
_CHAT_LOCK = threading.Lock()
_CHAT_REQUESTS = {}

def _check_creator_chat_rate(user_id):
    now = time.monotonic()
    with _CHAT_LOCK:
        recent = [
            t for t in _CHAT_REQUESTS.get(user_id, [])
            if now - t < 60
        ]
        if len(recent) >= 20:
            _CHAT_REQUESTS[user_id] = recent
            raise RuntimeError("chat_rate_limited")
        recent.append(now)
        _CHAT_REQUESTS[user_id] = recent


def _creator_db_get(table, params):
    from urllib.parse import urlencode

    base = os.environ.get("SUPABASE_URL", "").rstrip("/")
    secret = os.environ.get("SUPABASE_SECRET_KEY", "")
    if not base or not secret:
        raise RuntimeError("database_not_configured")

    parsed = urlsplit(base)
    if parsed.scheme != "https" or not parsed.hostname:
        raise RuntimeError("database_not_configured")

    url = base + "/rest/v1/" + table + "?" + urlencode(params)
    request = Request(
        url,
        headers={"apikey": secret, "Accept": "application/json"},
    )

    try:
        with urlopen(request, timeout=15) as response:
            result = json.loads(response.read(100000).decode("utf-8"))
        if not isinstance(result, list):
            raise RuntimeError("database_response_invalid")
        return result
    except HTTPError as error:
        error.read(1024)
        raise RuntimeError("database_operation_failed")
    except (URLError, UnicodeDecodeError, json.JSONDecodeError):
        raise RuntimeError("database_unavailable")


def _send_to_creator_ai(connection_data, messages):
    endpoint = connection_data.get("endpoint_url", "")
    model = connection_data.get("model", "")
    auth_type = connection_data.get("auth_type", "")
    header_name = connection_data.get("header_name") or ""
    encrypted = connection_data.get("encrypted_api_key") or ""
    api_key = decrypt_api_key(encrypted) if encrypted else ""

    host, ip, path = _validate_endpoint(endpoint)

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    if auth_type == "bearer":
        if not api_key:
            raise RuntimeError("creator_credentials_unavailable")
        headers["Authorization"] = "Bearer " + api_key

    elif auth_type == "header":
        if (
            not api_key
            or not re.fullmatch(r"[A-Za-z0-9-]{1,64}", header_name)
            or header_name.lower() in {
                "host", "content-length", "connection",
                "transfer-encoding", "content-type",
            }
        ):
            raise RuntimeError("creator_credentials_unavailable")
        headers[header_name] = api_key

    elif auth_type != "none":
        raise RuntimeError("creator_connection_invalid")

    body = json.dumps({
        "model": model,
        "messages": messages,
        "max_tokens": 512,
    }).encode("utf-8")

    client = _PinnedHTTPSConnection(host, ip, 25)
    try:
        client.request("POST", path, body=body, headers=headers)
        response = client.getresponse()

        if response.status < 200 or response.status >= 300:
            response.read(1024)
            raise RuntimeError("creator_api_unavailable")

        raw = response.read(65537)
        if len(raw) > 65536:
            raise RuntimeError("creator_api_response_too_large")

        result = json.loads(raw.decode("utf-8"))
        choices = result.get("choices") if isinstance(result, dict) else None

        if not isinstance(choices, list) or not choices:
            raise RuntimeError("creator_api_response_invalid")

        message = choices[0].get("message", {})
        answer = message.get("content", "") if isinstance(message, dict) else ""

        if not isinstance(answer, str) or not answer.strip():
            raise RuntimeError("creator_api_response_invalid")

        return answer.strip()

    except (UnicodeDecodeError, json.JSONDecodeError):
        raise RuntimeError("creator_api_response_invalid")
    except (OSError, http.client.HTTPException, ssl.SSLError):
        raise RuntimeError("creator_api_unavailable")
    finally:
        client.close()


def handle_creator_ai_chat(handler):
    try:
        user_id = _authenticate(
            handler.headers.get("Authorization", "")
        )
        _check_creator_chat_rate(user_id)

        payload = _read_payload(handler)
        ai_id = str(payload.get("ai_id", "")).strip()

        if not re.fullmatch(
            r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-"
            r"[1-8][0-9a-fA-F]{3}-[89aAbB][0-9a-fA-F]{3}-"
            r"[0-9a-fA-F]{12}",
            ai_id,
        ):
            raise ValueError("invalid_ai_id")

        raw_messages = payload.get("messages")
        if not isinstance(raw_messages, list) or not 1 <= len(raw_messages) <= 20:
            raise ValueError("invalid_messages")

        messages = []
        total = 0

        for item in raw_messages:
            if not isinstance(item, dict):
                raise ValueError("invalid_messages")

            role = item.get("role")
            content = item.get("content", item.get("text", ""))

            if role not in ("user", "assistant") or not isinstance(content, str):
                raise ValueError("invalid_messages")

            content = content.strip()
            total += len(content)

            if not content or len(content) > 4000 or total > 16000:
                raise ValueError("message_too_large")

            messages.append({"role": role, "content": content})

        if messages[-1]["role"] != "user":
            raise ValueError("last_message_must_be_user")

        # Only publicly published AIs can be used by other users.
        submissions = _creator_db_get("ai_submissions", {
            "id": "eq." + ai_id,
            "status": "eq.published",
            "select": "id",
            "limit": "1",
        })

        if not submissions:
            handler.send_json(404, {"error": "This AI is not available."})
            return

        connections = _creator_db_get("ai_creator_connections", {
            "submission_id": "eq." + ai_id,
            "status": "eq.active",
            "select": "endpoint_url,model,auth_type,header_name,encrypted_api_key",
            "limit": "1",
        })

        if not connections:
            handler.send_json(404, {"error": "AI connection unavailable."})
            return

        answer = _send_to_creator_ai(connections[0], messages)
        handler.send_json(200, {"ok": True, "response": answer})

    except PermissionError:
        handler.send_json(401, {"error": "Please sign in again."})

    except ValueError as error:
        handler.send_json(400, {"error": str(error)})

    except RuntimeError as error:
        if str(error) == "chat_rate_limited":
            handler.send_json(429, {
                "error": "Too many messages. Please wait a minute."
            })
        else:
            handler.send_json(502, {
                "error": "The creator AI is temporarily unavailable."
            })

    except Exception:
        handler.send_json(502, {
            "error": "The creator AI is temporarily unavailable."
        })


def handle_list_published_creator_ais(handler):
    """Return safe public details for published creator AIs."""
    try:
        rows = _creator_db_get(
            "ai_submissions",
            {
                "select": "id,name,description,category,api_provider,created_at",
                "status": "eq.published",
                "submission_type": "eq.api_connection",
                "order": "created_at.desc",
                "limit": "100",
            },
        )

        public_ais = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            public_ais.append({
                "id": str(row.get("id", "")),
                "name": str(row.get("name", "")),
                "description": str(row.get("description", "")),
                "category": str(row.get("category", "")),
                "provider": str(row.get("api_provider", "")),
            })

        handler.send_json(200, {"ok": True, "ais": public_ais})

    except Exception:
        handler.send_json(502, {
            "ok": False,
            "error": "Could not load published creator AIs.",
        })


# ------------------------------------------------------------
# OUR SEARCH temporary Test AI API
# ------------------------------------------------------------

_MOCK_AI_LOCK = threading.Lock()
_MOCK_AI_REQUESTS = {}


def handle_mock_creator_ai(handler):
    try:
        auth = handler.headers.get("Authorization", "")

        if auth != "Bearer our-search-test-key":
            handler.send_json(401, {
                "error": {"message": "Invalid test API key."}
            })
            return

        client_ip = handler.client_address[0]
        now = time.time()

        with _MOCK_AI_LOCK:
            recent = [
                stamp
                for stamp in _MOCK_AI_REQUESTS.get(client_ip, [])
                if now - stamp < 60
            ]

            if len(recent) >= 20:
                handler.send_json(429, {
                    "error": {"message": "Test API rate limit reached."}
                })
                return

            recent.append(now)
            _MOCK_AI_REQUESTS[client_ip] = recent

        payload = _read_payload(handler)

        model = payload.get("model")
        messages = payload.get("messages")

        if (
            not isinstance(model, str)
            or not model
            or len(model) > 120
            or not isinstance(messages, list)
            or not messages
            or len(messages) > 20
        ):
            raise ValueError("invalid_test_request")

        for message in messages:
            if (
                not isinstance(message, dict)
                or message.get("role") not in ("system", "user", "assistant")
                or not isinstance(message.get("content"), str)
                or len(message["content"]) > 4000
            ):
                raise ValueError("invalid_test_message")

        handler.send_json(200, {
            "id": "our-search-test-completion",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": (
                            "Hello! This is the OUR SEARCH Test AI. "
                            "Your API connection is working successfully."
                        ),
                    },
                    "finish_reason": "stop",
                }
            ],
        })

    except ValueError as error:
        handler.send_json(400, {
            "error": {"message": str(error)}
        })

    except Exception:
        handler.send_json(500, {
            "error": {"message": "Test AI service error."}
        })
