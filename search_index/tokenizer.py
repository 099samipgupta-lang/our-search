import re
import unicodedata


class Tokenizer:

    TOKEN_PATTERN = re.compile(
        r"[^\W_]+(?:['’-][^\W_]+)*",
        re.UNICODE
    )

    def normalize_text(self, text):

        if not isinstance(text, str):
            return ""

        text = unicodedata.normalize(
            "NFKC",
            text
        )

        text = text.casefold()

        return text

    def tokenize(self, text):

        text = self.normalize_text(
            text
        )

        return [
            match.group(0)
            for match in self.TOKEN_PATTERN.finditer(
                text
            )
        ]

    def tokenize_with_positions(self, text):

        tokens = self.tokenize(text)

        return [
            {
                "term": term,
                "position": position
            }
            for position, term in enumerate(tokens)
        ]
