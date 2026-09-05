from urllib.parse import (
    urlsplit,
    urlunsplit,
    parse_qsl,
    urlencode,
    quote,
    unquote
)


TRACKING_PARAMETERS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "gclid",
    "dclid",
    "fbclid",
    "msclkid",
    "mc_cid",
    "mc_eid"
}


class URLNormalizer:

    def normalize(self, url):

        if not isinstance(url, str):
            return None

        url = url.strip()

        if not url:
            return None

        try:

            parts = urlsplit(url)

        except Exception:

            return None

        scheme = parts.scheme.lower()

        if scheme not in (
            "http",
            "https"
        ):
            return None

        if not parts.netloc:
            return None

        hostname = parts.hostname

        if not hostname:
            return None

        hostname = hostname.rstrip(".").lower()

        try:
            hostname = hostname.encode(
                "idna"
            ).decode(
                "ascii"
            )
        except Exception:
            pass

        username = parts.username
        password = parts.password

        userinfo = ""

        if username is not None:

            userinfo = quote(
                username,
                safe=""
            )

            if password is not None:

                userinfo += ":"

                userinfo += quote(
                    password,
                    safe=""
                )

            userinfo += "@"

        port = parts.port

        if port is not None:

            if not (
                (scheme == "http" and port == 80)
                or
                (scheme == "https" and port == 443)
            ):

                hostname = (
                    hostname
                    + ":"
                    + str(port)
                )

        netloc = (
            userinfo
            + hostname
        )

        path = self._normalize_path(
            parts.path
        )

        query = self._normalize_query(
            parts.query
        )

        # Fragments are never sent to the server.
        fragment = ""

        return urlunsplit(
            (
                scheme,
                netloc,
                path,
                query,
                fragment
            )
        )

    def _normalize_path(self, path):

        if not path:
            return "/"

        path = unquote(
            path
        )

        segments = []

        for segment in path.split("/"):

            if segment == "":
                continue

            if segment == ".":
                continue

            if segment == "..":

                if segments:
                    segments.pop()

                continue

            segments.append(
                segment
            )

        normalized = "/".join(
            segments
        )

        if not normalized:
            return "/"

        return "/" + normalized

    def _normalize_query(self, query):

        if not query:
            return ""

        try:

            parameters = parse_qsl(
                query,
                keep_blank_values=True
            )

        except Exception:

            return query

        filtered = []

        for key, value in parameters:

            key_lower = key.lower()

            if key_lower in (
                TRACKING_PARAMETERS
            ):
                continue

            filtered.append(
                (
                    key,
                    value
                )
            )

        # Sort only by parameter name/value.
        # This gives deterministic URLs.
        filtered.sort(
            key=lambda item: (
                item[0],
                item[1]
            )
        )

        return urlencode(
            filtered,
            doseq=True
        )
