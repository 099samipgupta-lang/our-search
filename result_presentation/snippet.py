import re


class SnippetGenerator:
    def __init__(self, max_length=180):
        self.max_length = max(40, int(max_length))

    def _clean(self, text):
        if not isinstance(text, str):
            return ""

        return " ".join(text.split())

    def _find_match(self, text, terms):
        lowered = text.casefold()

        best_position = None
        best_term = None

        for term in terms:
            if not term:
                continue

            position = lowered.find(term.casefold())

            if position == -1:
                continue

            if best_position is None or position < best_position:
                best_position = position
                best_term = term

        return best_position, best_term

    def generate(self, text, terms):
        text = self._clean(text)

        if not text:
            return ""

        position, term = self._find_match(
            text,
            terms,
        )

        if position is None:
            return text[:self.max_length].rstrip()

        half = self.max_length // 2

        start = max(
            0,
            position - half,
        )

        end = min(
            len(text),
            start + self.max_length,
        )

        if end - start < self.max_length:
            start = max(
                0,
                end - self.max_length,
            )

        snippet = text[start:end].strip()

        if start > 0:
            snippet = "… " + snippet

        if end < len(text):
            snippet = snippet.rstrip() + " …"

        return snippet

    def highlight(self, snippet, terms):
        if not snippet:
            return ""

        result = snippet

        clean_terms = sorted(
            {
                term
                for term in terms
                if isinstance(term, str) and term
            },
            key=len,
            reverse=True,
        )

        for term in clean_terms:
            pattern = re.compile(
                re.escape(term),
                re.IGNORECASE,
            )

            result = pattern.sub(
                lambda match: "["
                + match.group(0)
                + "]",
                result,
            )

        return result
