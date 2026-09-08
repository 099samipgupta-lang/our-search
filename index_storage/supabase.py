import json
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from index_storage.backend import IndexStorageBackend


class SupabaseIndexStorage(IndexStorageBackend):

    def __init__(
        self,
        project_url,
        api_key,
        bucket,
        timeout=30,
    ):

        if not isinstance(project_url, str):
            raise TypeError(
                "project_url must be a string"
            )

        if not isinstance(api_key, str):
            raise TypeError(
                "api_key must be a string"
            )

        if not isinstance(bucket, str):
            raise TypeError(
                "bucket must be a string"
            )

        project_url = (
            project_url.strip().rstrip("/")
        )

        api_key = api_key.strip()
        bucket = bucket.strip()

        if not project_url:
            raise ValueError(
                "project_url must not be empty"
            )

        if not api_key:
            raise ValueError(
                "api_key must not be empty"
            )

        if not bucket:
            raise ValueError(
                "bucket must not be empty"
            )

        self.project_url = project_url
        self.api_key = api_key
        self.bucket = bucket
        self.timeout = max(
            1,
            int(timeout),
        )

    def _headers(self, extra=None):

        headers = {
            "Authorization": (
                "Bearer " + self.api_key
            ),
            "apikey": self.api_key,
        }

        if extra:
            headers.update(extra)

        return headers

    def _object_url(self, key):

        return (
            self.project_url
            + "/storage/v1/object/"
            + quote(
                self.bucket,
                safe="",
            )
            + "/"
            + quote(
                key,
                safe="/",
            )
        )

    def put(self, key, data):

        if not isinstance(data, bytes):
            raise TypeError(
                "data must be bytes"
            )

        request = Request(
            self._object_url(key),
            data=data,
            method="POST",
            headers=self._headers(
                {
                    "Content-Type":
                        "application/octet-stream",
                    "x-upsert":
                        "true",
                }
            ),
        )

        with urlopen(
            request,
            timeout=self.timeout,
        ) as response:

            response.read()

    def get(self, key):

        request = Request(
            self._object_url(key),
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

            if error.code == 400:
                try:
                    payload = json.loads(
                        error.read().decode(
                            "utf-8"
                        )
                    )

                    if payload.get("code") == "NoSuchKey":
                        return None

                except Exception:
                    pass

            raise

    def exists(self, key):

        request = Request(
            self._object_url(key),
            method="HEAD",
            headers=self._headers(),
        )

        try:

            with urlopen(
                request,
                timeout=self.timeout,
            ) as response:

                return response.status == 200

        except HTTPError as error:

            if error.code == 404:
                return False

            raise

    def delete(self, key):

        request = Request(
            self._object_url(key),
            method="DELETE",
            headers=self._headers(),
        )

        try:

            with urlopen(
                request,
                timeout=self.timeout,
            ) as response:

                response.read()
                return True

        except HTTPError as error:

            if error.code == 404:
                return False

            raise

    def list_keys(self, prefix=""):

        url = (
            self.project_url
            + "/storage/v1/object/list/"
            + quote(
                self.bucket,
                safe="",
            )
        )

        payload = json.dumps(
            {
                "prefix": prefix,
                "limit": 1000,
                "offset": 0,
            }
        ).encode("utf-8")

        request = Request(
            url,
            data=payload,
            method="POST",
            headers=self._headers(
                {
                    "Content-Type":
                        "application/json",
                }
            ),
        )

        with urlopen(
            request,
            timeout=self.timeout,
        ) as response:

            items = json.loads(
                response.read().decode(
                    "utf-8"
                )
            )

        keys = []

        for item in items:

            name = item.get("name")

            if not name:
                continue

            key = (
                prefix.rstrip("/")
                + "/"
                + name
                if prefix
                else name
            )

            keys.append(key)

        return keys
