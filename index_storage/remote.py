import json
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from index_storage.backend import IndexStorageBackend


class RemoteIndexStorage(IndexStorageBackend):

    def __init__(
        self,
        base_url,
        api_key=None,
        timeout=30,
    ):

        if not isinstance(
            base_url,
            str,
        ):
            raise TypeError(
                "base_url must be a string"
            )

        base_url = base_url.strip().rstrip("/")

        if not base_url:
            raise ValueError(
                "base_url must not be empty"
            )

        if api_key is not None and not isinstance(
            api_key,
            str,
        ):
            raise TypeError(
                "api_key must be a string or None"
            )

        self.base_url = base_url
        self.api_key = api_key
        self.timeout = max(
            1,
            int(timeout),
        )

    def _headers(
        self,
        extra=None,
    ):

        headers = {}

        if self.api_key:
            headers[
                "X-Storage-API-Key"
            ] = self.api_key

        if extra:
            headers.update(extra)

        return headers

    def _url(
        self,
        path,
    ):

        return self.base_url + path

    def put(
        self,
        key,
        data,
    ):

        if not isinstance(
            data,
            bytes,
        ):
            raise TypeError(
                "data must be bytes"
            )

        request = Request(
            self._url("/put"),
            data=data,
            method="PUT",
            headers=self._headers(
                {
                    "X-Storage-Key": key,
                    "Content-Type":
                        "application/octet-stream",
                }
            ),
        )

        with urlopen(
            request,
            timeout=self.timeout,
        ) as response:

            response.read()

    def get(
        self,
        key,
    ):

        url = (
            self._url("/get?key=")
            + quote(
                key,
                safe="",
            )
        )

        request = Request(
            url,
            method="GET",
            headers=self._headers(),
        )

        try:

            with urlopen(
                request,
                timeout=self.timeout,
            ) as response:

                return response.read()

        except HTTPError as error:

            if error.code == 404:
                return None

            raise

    def exists(
        self,
        key,
    ):

        url = (
            self._url("/exists?key=")
            + quote(
                key,
                safe="",
            )
        )

        request = Request(
            url,
            method="GET",
            headers=self._headers(),
        )

        with urlopen(
            request,
            timeout=self.timeout,
        ) as response:

            payload = json.loads(
                response.read().decode(
                    "utf-8"
                )
            )

            return bool(
                payload["exists"]
            )

    def delete(
        self,
        key,
    ):

        request = Request(
            self._url("/delete"),
            method="DELETE",
            headers=self._headers(
                {
                    "X-Storage-Key": key,
                }
            ),
        )

        with urlopen(
            request,
            timeout=self.timeout,
        ) as response:

            payload = json.loads(
                response.read().decode(
                    "utf-8"
                )
            )

            return bool(
                payload["deleted"]
            )

    def list_keys(
        self,
        prefix="",
    ):

        url = (
            self._url("/list?prefix=")
            + quote(
                prefix,
                safe="",
            )
        )

        request = Request(
            url,
            method="GET",
            headers=self._headers(),
        )

        with urlopen(
            request,
            timeout=self.timeout,
        ) as response:

            payload = json.loads(
                response.read().decode(
                    "utf-8"
                )
            )

            return list(
                payload["keys"]
            )
