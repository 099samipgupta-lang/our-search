import ssl
import threading
import time

from http.client import (
    HTTPConnection,
    HTTPSConnection,
)

from urllib.error import (
    HTTPError,
    URLError,
)

from urllib.parse import (
    urljoin,
    urlsplit,
)


REDIRECT_STATUSES = {
    301,
    302,
    303,
    307,
    308,
}


RETRY_STATUSES = {
    408,
    425,
    429,
    500,
    502,
    503,
    504,
}


class _ConnectionPool:

    def __init__(
        self,
        scheme,
        hostname,
        port,
        max_connections,
        timeout,
        ssl_context=None,
    ):

        self.scheme = scheme
        self.hostname = hostname
        self.port = port

        self.max_connections = max(
            1,
            int(max_connections),
        )

        self.timeout = float(timeout)
        self.ssl_context = ssl_context

        self._available = []
        self._created = 0

        self._condition = threading.Condition(
            threading.RLock()
        )

        self.total_created = 0
        self.total_reused = 0
        self.total_discarded = 0

    def _create_connection(self):

        if self.scheme == "https":

            connection = HTTPSConnection(
                self.hostname,
                self.port,
                timeout=self.timeout,
                context=self.ssl_context,
            )

        else:

            connection = HTTPConnection(
                self.hostname,
                self.port,
                timeout=self.timeout,
            )

        self._created += 1
        self.total_created += 1

        return connection

    def acquire(self):

        deadline = (
            time.monotonic()
            + self.timeout
        )

        with self._condition:

            while True:

                if self._available:

                    connection = (
                        self._available.pop()
                    )

                    self.total_reused += 1

                    return connection

                if (
                    self._created
                    < self.max_connections
                ):

                    return self._create_connection()

                remaining = (
                    deadline
                    - time.monotonic()
                )

                if remaining <= 0:

                    raise TimeoutError(
                        "Connection pool "
                        "acquire timeout"
                    )

                self._condition.wait(
                    timeout=remaining
                )

    def release(
        self,
        connection,
    ):

        if connection is None:
            return

        with self._condition:

            if self._created <= 0:

                try:
                    connection.close()
                except Exception:
                    pass

                return

            self._available.append(
                connection
            )

            self._condition.notify()

    def discard(
        self,
        connection,
    ):

        if connection is not None:

            try:
                connection.close()
            except Exception:
                pass

        with self._condition:

            if self._created > 0:
                self._created -= 1

            self.total_discarded += 1

            self._condition.notify()

    def close(self):

        with self._condition:

            connections = list(
                self._available
            )

            self._available.clear()

            self._created = max(
                0,
                self._created
                - len(connections),
            )

            for connection in connections:

                try:
                    connection.close()
                except Exception:
                    pass

            self._condition.notify_all()

    def stats(self):

        with self._condition:

            return {
                "scheme":
                    self.scheme,
                "hostname":
                    self.hostname,
                "port":
                    self.port,
                "max_connections":
                    self.max_connections,
                "created":
                    self._created,
                "available":
                    len(self._available),
                "in_use":
                    (
                        self._created
                        - len(self._available)
                    ),
                "total_created":
                    self.total_created,
                "total_reused":
                    self.total_reused,
                "total_discarded":
                    self.total_discarded,
            }


class Fetcher:

    def __init__(
        self,
        user_agent="OurSearchBot/1.0",
        max_redirects=10,
        max_retries=3,
        backoff_base=1.0,
        timeout=10.0,
        max_body_bytes=10 * 1024 * 1024,
        max_connections_per_origin=4,
    ):

        self.user_agent = user_agent

        self.max_redirects = int(
            max_redirects
        )

        self.max_retries = int(
            max_retries
        )

        self.backoff_base = float(
            backoff_base
        )

        self.timeout = float(
            timeout
        )

        self.max_body_bytes = int(
            max_body_bytes
        )

        self.max_connections_per_origin = max(
            1,
            int(max_connections_per_origin),
        )

        self._ssl_context = (
            ssl.create_default_context()
        )

        self._pools = {}

        self._pools_lock = threading.RLock()

        self._closed = False

    # ---------------------------------------------------------
    # Origin handling
    # ---------------------------------------------------------

    @staticmethod
    def _origin(url):

        parsed = urlsplit(url)

        scheme = parsed.scheme.lower()

        hostname = parsed.hostname

        if not hostname:

            raise ValueError(
                f"URL has no hostname: {url}"
            )

        hostname = hostname.lower()

        if parsed.port is not None:

            port = parsed.port

        elif scheme == "https":

            port = 443

        else:

            port = 80

        return (
            scheme,
            hostname,
            port,
        )

    def _pool_for_url(
        self,
        url,
    ):

        origin = self._origin(
            url
        )

        with self._pools_lock:

            pool = self._pools.get(
                origin
            )

            if pool is None:

                pool = _ConnectionPool(
                    scheme=origin[0],
                    hostname=origin[1],
                    port=origin[2],
                    max_connections=(
                        self.max_connections_per_origin
                    ),
                    timeout=self.timeout,
                    ssl_context=(
                        self._ssl_context
                    ),
                )

                self._pools[
                    origin
                ] = pool

            return pool

    # ---------------------------------------------------------
    # Transport
    # ---------------------------------------------------------

    def _request(
        self,
        url,
        etag=None,
        last_modified=None,
    ):

        if self._closed:

            raise RuntimeError(
                "Fetcher is closed"
            )

        parsed = urlsplit(
            url
        )

        if parsed.scheme not in (
            "http",
            "https",
        ):

            raise ValueError(
                f"Unsupported URL scheme: "
                f"{parsed.scheme}"
            )

        pool = self._pool_for_url(
            url
        )

        connection = pool.acquire()

        try:

            headers = {
                "User-Agent":
                    self.user_agent,
                "Connection":
                    "keep-alive",
            }

            if etag:

                headers[
                    "If-None-Match"
                ] = etag

            if last_modified:

                headers[
                    "If-Modified-Since"
                ] = last_modified

            path = (
                parsed.path
                or "/"
            )

            if parsed.query:

                path += (
                    "?"
                    + parsed.query
                )

            connection.request(
                "GET",
                path,
                headers=headers,
            )

            response = (
                connection.getresponse()
            )

            return (
                pool,
                connection,
                response,
            )

        except Exception:

            pool.discard(
                connection
            )

            raise

    # ---------------------------------------------------------
    # Body protection
    # ---------------------------------------------------------

    def _read_body(
        self,
        response,
    ):

        content_length = (
            response.getheader(
                "Content-Length"
            )
        )

        if content_length:

            try:

                declared_size = int(
                    content_length
                )

            except ValueError:

                declared_size = None

            if (
                declared_size is not None
                and declared_size
                > self.max_body_bytes
            ):

                return (
                    None,
                    True,
                )

        chunks = []

        total = 0

        while True:

            remaining = (
                self.max_body_bytes
                + 1
                - total
            )

            if remaining <= 0:

                return (
                    None,
                    True,
                )

            chunk = response.read(
                min(
                    64 * 1024,
                    remaining,
                )
            )

            if not chunk:
                break

            chunks.append(
                chunk
            )

            total += len(
                chunk
            )

            if (
                total
                > self.max_body_bytes
            ):

                return (
                    None,
                    True,
                )

        return (
            b"".join(chunks),
            False,
        )

    # ---------------------------------------------------------
    # Retry
    # ---------------------------------------------------------

    def _backoff(
        self,
        retry_number,
    ):

        delay = (
            self.backoff_base
            * (
                2 ** (
                    retry_number - 1
                )
            )
        )

        time.sleep(
            delay
        )

    # ---------------------------------------------------------
    # Result
    # ---------------------------------------------------------

    @staticmethod
    def _validator_values(
        headers
    ):

        return {
            "etag":
                headers.get(
                    "ETag"
                ),
            "last_modified":
                headers.get(
                    "Last-Modified"
                ),
        }

    def _result(
        self,
        url,
        requested_url,
        redirect_chain,
        status,
        status_type,
        content_type,
        headers,
        body,
        retries,
    ):

        validators = (
            self._validator_values(
                headers
            )
        )

        return {
            "url":
                url,
            "requested_url":
                requested_url,
            "status":
                status,
            "status_type":
                status_type,
            "content_type":
                content_type,
            "headers":
                headers,
            "body":
                body,
            "redirect_chain":
                redirect_chain,
            "final_url":
                url,
            "retries":
                retries,
            "etag":
                validators[
                    "etag"
                ],
            "last_modified":
                validators[
                    "last_modified"
                ],
        }

    # ---------------------------------------------------------
    # Fetch
    # ---------------------------------------------------------

    def fetch(
        self,
        url,
        etag=None,
        last_modified=None,
    ):

        current_url = url

        redirect_chain = []

        visited_redirects = set()

        retries = 0

        redirects = 0

        while True:

            if current_url in (
                visited_redirects
            ):

                return self._result(
                    current_url,
                    url,
                    redirect_chain,
                    0,
                    "redirect_loop",
                    "",
                    {},
                    b"",
                    retries,
                )

            visited_redirects.add(
                current_url
            )

            request_etag = (
                etag
                if current_url == url
                else None
            )

            request_last_modified = (
                last_modified
                if current_url == url
                else None
            )

            pool = None

            connection = None

            reusable = False

            try:

                (
                    pool,
                    connection,
                    response,
                ) = self._request(
                    current_url,
                    request_etag,
                    request_last_modified,
                )

                status = response.status

                headers = dict(
                    response.getheaders()
                )

                content_type = (
                    response.getheader(
                        "Content-Type",
                        "",
                    )
                )

                body, oversized = (
                    self._read_body(
                        response
                    )
                )

                if oversized:

                    pool.discard(
                        connection
                    )

                    connection = None

                    return self._result(
                        current_url,
                        url,
                        redirect_chain,
                        status,
                        "body_too_large",
                        content_type,
                        headers,
                        b"",
                        retries,
                    )

                connection_header = (
                    response.getheader(
                        "Connection",
                        "",
                    ).lower()
                )

                if (
                    connection_header
                    == "close"
                ):

                    reusable = False

                else:

                    reusable = True

                if reusable:
                    pool.release(connection)
                else:
                    pool.discard(connection)

                connection = None

                if status == 304:

                    return self._result(
                        current_url,
                        url,
                        redirect_chain,
                        status,
                        "not_modified",
                        content_type,
                        headers,
                        b"",
                        retries,
                    )

                if status in (
                    REDIRECT_STATUSES
                ):

                    location = (
                        headers.get(
                            "Location"
                        )
                    )

                    if not location:

                        return self._result(
                            current_url,
                            url,
                            redirect_chain,
                            status,
                            "redirect",
                            content_type,
                            headers,
                            body,
                            retries,
                        )

                    redirects += 1

                    if (
                        redirects
                        > self.max_redirects
                    ):

                        return self._result(
                            current_url,
                            url,
                            redirect_chain,
                            status,
                            "too_many_redirects",
                            content_type,
                            headers,
                            b"",
                            retries,
                        )

                    redirect_url = (
                        self._absolute_url(
                            current_url,
                            location,
                        )
                    )

                    redirect_chain.append(
                        {
                            "from":
                                current_url,
                            "status":
                                status,
                            "to":
                                redirect_url,
                        }
                    )

                    current_url = (
                        redirect_url
                    )

                    continue

                if (
                    status in RETRY_STATUSES
                    and retries
                    < self.max_retries
                ):

                    retries += 1

                    self._backoff(
                        retries
                    )

                    continue

                return self._result(
                    current_url,
                    url,
                    redirect_chain,
                    status,
                    self.classify_status(
                        status
                    ),
                    content_type,
                    headers,
                    body,
                    retries,
                )

            except HTTPError as error:

                status = error.code

                headers = dict(
                    error.headers
                )

                content_type = (
                    error.headers.get(
                        "Content-Type",
                        "",
                    )
                )

                location = (
                    error.headers.get(
                        "Location"
                    )
                )

                if status == 304:

                    return self._result(
                        current_url,
                        url,
                        redirect_chain,
                        status,
                        "not_modified",
                        content_type,
                        headers,
                        b"",
                        retries,
                    )

                if status in (
                    REDIRECT_STATUSES
                ):

                    if not location:

                        return self._result(
                            current_url,
                            url,
                            redirect_chain,
                            status,
                            "redirect",
                            content_type,
                            headers,
                            b"",
                            retries,
                        )

                    redirects += 1

                    if (
                        redirects
                        > self.max_redirects
                    ):

                        return self._result(
                            current_url,
                            url,
                            redirect_chain,
                            status,
                            "too_many_redirects",
                            content_type,
                            headers,
                            b"",
                            retries,
                        )

                    redirect_url = (
                        self._absolute_url(
                            current_url,
                            location,
                        )
                    )

                    redirect_chain.append(
                        {
                            "from":
                                current_url,
                            "status":
                                status,
                            "to":
                                redirect_url,
                        }
                    )

                    current_url = (
                        redirect_url
                    )

                    continue

                if (
                    status in RETRY_STATUSES
                    and retries
                    < self.max_retries
                ):

                    retries += 1

                    self._backoff(
                        retries
                    )

                    continue

                body = error.read()

                return self._result(
                    current_url,
                    url,
                    redirect_chain,
                    status,
                    self.classify_status(
                        status
                    ),
                    content_type,
                    headers,
                    body,
                    retries,
                )

            except (
                URLError,
                TimeoutError,
                ConnectionError,
                OSError,
            ) as error:

                if (
                    pool is not None
                    and connection is not None
                ):

                    pool.discard(
                        connection
                    )

                    connection = None

                error_text = str(error)

                error_type = type(error).__name__

                error_reason = getattr(
                    error,
                    "reason",
                    None,
                )

                error_errno = getattr(
                    error,
                    "errno",
                    None,
                )

                if (
                    error_reason is not None
                    and error_reason is not error
                ):

                    reason_text = str(
                        error_reason
                    )

                else:

                    reason_text = None

                diagnostic = {
                    "type":
                        error_type,
                    "message":
                        error_text,
                    "reason":
                        reason_text,
                    "errno":
                        error_errno,
                }

                if retries < self.max_retries:

                    retries += 1

                    self._backoff(
                        retries
                    )

                    continue

                return {
                    "url":
                        current_url,
                    "requested_url":
                        url,
                    "status":
                        0,
                    "status_type":
                        "network_error",
                    "content_type":
                        "",
                    "headers":
                        {},
                    "body":
                        b"",
                    "redirect_chain":
                        redirect_chain,
                    "final_url":
                        current_url,
                    "retries":
                        retries,
                    "etag":
                        None,
                    "last_modified":
                        None,
                    "error":
                        error_text,
                    "error_type":
                        error_type,
                    "error_reason":
                        reason_text,
                    "error_errno":
                        error_errno,
                    "error_diagnostic":
                        diagnostic,
                }

            except Exception as error:

                if (
                    pool is not None
                    and connection is not None
                ):

                    pool.discard(
                        connection
                    )

                    connection = None

                return {
                    "url":
                        current_url,
                    "requested_url":
                        url,
                    "status":
                        0,
                    "status_type":
                        "fetch_error",
                    "content_type":
                        "",
                    "headers":
                        {},
                    "body":
                        b"",
                    "redirect_chain":
                        redirect_chain,
                    "final_url":
                        current_url,
                    "retries":
                        retries,
                    "etag":
                        None,
                    "last_modified":
                        None,
                    "error":
                        str(error),
                }

            finally:

                if (
                    pool is not None
                    and connection is not None
                ):

                    if reusable:
                        pool.release(
                            connection
                        )
                    else:
                        pool.discard(
                            connection
                        )

    # ---------------------------------------------------------
    # Pool diagnostics
    # ---------------------------------------------------------

    def pool_stats(self):

        with self._pools_lock:

            return {
                origin:
                    pool.stats()
                for origin, pool
                in self._pools.items()
            }

    # ---------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------

    def close(self):

        if self._closed:
            return

        self._closed = True

        with self._pools_lock:

            for pool in (
                self._pools.values()
            ):

                pool.close()

            self._pools.clear()

    def __enter__(self):

        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ):

        self.close()

    # ---------------------------------------------------------
    # URL helpers
    # ---------------------------------------------------------

    @staticmethod
    def _absolute_url(
        base_url,
        location,
    ):

        return urljoin(
            base_url,
            location,
        )

    @staticmethod
    def classify_status(
        status
    ):

        if status == 304:
            return "not_modified"

        if 200 <= status < 300:
            return "success"

        if 300 <= status < 400:
            return "redirect"

        if status == 404:
            return "not_found"

        if status == 410:
            return "gone"

        if 400 <= status < 500:
            return "client_error"

        if 500 <= status < 600:
            return "server_error"

        return "unknown"
