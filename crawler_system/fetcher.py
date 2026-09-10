import time

from urllib.parse import urljoin

from urllib.request import (
    Request,
    build_opener,
    HTTPRedirectHandler
)

from urllib.error import (
    HTTPError,
    URLError
)


REDIRECT_STATUSES = {
    301,
    302,
    303,
    307,
    308
}


RETRY_STATUSES = {
    408,
    425,
    429,
    500,
    502,
    503,
    504
}


class NoRedirectHandler(
    HTTPRedirectHandler
):

    def redirect_request(
        self,
        req,
        fp,
        code,
        msg,
        headers,
        newurl
    ):
        return None


class Fetcher:

    def __init__(
        self,
        user_agent="OurSearchBot/1.0",
        max_redirects=10,
        max_retries=3,
        backoff_base=1.0
    ):

        self.user_agent = user_agent
        self.max_redirects = (
            int(max_redirects)
        )
        self.max_retries = (
            int(max_retries)
        )
        self.backoff_base = (
            float(backoff_base)
        )

        self.opener = build_opener(
            NoRedirectHandler()
        )

    def _request(
        self,
        url,
        etag=None,
        last_modified=None
    ):

        headers = {
            "User-Agent":
                self.user_agent
        }

        if etag:

            headers[
                "If-None-Match"
            ] = etag

        if last_modified:

            headers[
                "If-Modified-Since"
            ] = last_modified

        request = Request(
            url,
            headers=headers
        )

        return self.opener.open(
            request,
            timeout=10
        )

    def _backoff(
        self,
        retry_number
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
                )
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
        retries
    ):

        validators = (
            self._validator_values(
                headers
            )
        )

        return {
            "url": url,
            "requested_url":
                requested_url,
            "status": status,
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
                validators["etag"],
            "last_modified":
                validators[
                    "last_modified"
                ]
        }

    def fetch(
        self,
        url,
        etag=None,
        last_modified=None
    ):

        current_url = url

        redirect_chain = []

        visited_redirects = set()

        retries = 0
        redirects = 0

        while True:

            if current_url in visited_redirects:

                return self._result(
                    current_url,
                    url,
                    redirect_chain,
                    0,
                    "redirect_loop",
                    "",
                    {},
                    b"",
                    retries
                )

            visited_redirects.add(
                current_url
            )

            # Validators belong to the
            # originally requested resource.
            #
            # Do not blindly send them to
            # a redirected URL, especially
            # if the redirect crosses hosts.
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

            try:

                response = self._request(
                    current_url,
                    request_etag,
                    request_last_modified
                )

                status = response.status

                headers = dict(
                    response.headers
                )

                content_type = (
                    response.headers.get(
                        "Content-Type",
                        ""
                    )
                )

                body = response.read()

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
                    retries
                )

            except HTTPError as error:

                status = error.code

                headers = dict(
                    error.headers
                )

                content_type = (
                    error.headers.get(
                        "Content-Type",
                        ""
                    )
                )

                location = (
                    error.headers.get(
                        "Location"
                    )
                )

                # 304 is returned by urllib as
                # HTTPError. It is nevertheless
                # a successful conditional fetch.
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
                        retries
                    )

                if status in REDIRECT_STATUSES:

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
                            retries
                        )

                    redirects += 1

                    if redirects > self.max_redirects:

                        return self._result(
                            current_url,
                            url,
                            redirect_chain,
                            status,
                            "too_many_redirects",
                            content_type,
                            headers,
                            b"",
                            retries
                        )

                    redirect_url = (
                        self._absolute_url(
                            current_url,
                            location
                        )
                    )

                    redirect_chain.append(
                        {
                            "from":
                                current_url,
                            "status":
                                status,
                            "to":
                                redirect_url
                        }
                    )

                    current_url = (
                        redirect_url
                    )

                    continue

                if (
                    status in RETRY_STATUSES
                    and retries < self.max_retries
                ):

                    retries += 1

                    print(
                        "Retry",
                        retries,
                        "/",
                        self.max_retries,
                        "- HTTP",
                        status,
                        "-",
                        current_url
                    )

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
                    retries
                )

            except (
                URLError,
                TimeoutError,
                ConnectionError
            ) as error:

                if retries < self.max_retries:

                    retries += 1

                    print(
                        "Retry",
                        retries,
                        "/",
                        self.max_retries,
                        "- network error -",
                        current_url
                    )

                    self._backoff(
                        retries
                    )

                    continue

                return {
                    "url":
                        current_url,
                    "requested_url":
                        url,
                    "status": 0,
                    "status_type":
                        "network_error",
                    "content_type": "",
                    "headers": {},
                    "body": b"",
                    "redirect_chain":
                        redirect_chain,
                    "final_url":
                        current_url,
                    "retries":
                        retries,
                    "etag": None,
                    "last_modified": None,
                    "error":
                        str(error)
                }

            except Exception as error:

                return {
                    "url":
                        current_url,
                    "requested_url":
                        url,
                    "status": 0,
                    "status_type":
                        "fetch_error",
                    "content_type": "",
                    "headers": {},
                    "body": b"",
                    "redirect_chain":
                        redirect_chain,
                    "final_url":
                        current_url,
                    "retries":
                        retries,
                    "etag": None,
                    "last_modified": None,
                    "error":
                        str(error)
                }

    @staticmethod
    def _absolute_url(
        base_url,
        location
    ):

        return urljoin(
            base_url,
            location
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
