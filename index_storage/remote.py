from urllib.parse import quote
from urllib.request import (
    Request,
    urlopen,
)

from index_storage.backend import IndexStorageBackend


class RemoteIndexStorage(IndexStorageBackend):

    def __init__(
        self,
        base_url,
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

        self.base_url = base_url
        self.timeout = timeout

    def _url(
        self,
        path,
    ):

        return (
            self.base_url
            + path
        )

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
            headers={
                "X-Storage-Key": key,
                "Content-Type":
                    "application/octet-stream",
            },
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
        )

        try:

            with urlopen(
                request,
                timeout=self.timeout,
            ) as response:

                return response.read()

        except Exception as error:

            if getattr(
                error,
                "code",
                None,
            ) == 404:

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
        )

        with urlopen(
            request,
            timeout=self.timeout,
        ) as response:

            import json

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
            headers={
                "X-Storage-Key": key,
            },
        )

        with urlopen(
            request,
            timeout=self.timeout,
        ) as response:

            import json

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
        )

        with urlopen(
            request,
            timeout=self.timeout,
        ) as response:

            import json

            payload = json.loads(
                response.read().decode(
                    "utf-8"
                )
            )

            return list(
                payload["keys"]
            )
