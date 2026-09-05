import re
from dataclasses import dataclass


@dataclass
class QueryToken:
    value: str
    token_type: str
    position: int


class QueryParser:
    TOKEN_PATTERN = re.compile(
        r'"[^"]*"|\(|\)|\bAND\b|\bOR\b|\bNOT\b|[^\s()]+',
        re.IGNORECASE
    )

    def parse(self, query):
        if not isinstance(query, str):
            raise TypeError("query must be a string")

        tokens = []

        for position, match in enumerate(
            self.TOKEN_PATTERN.finditer(query)
        ):
            value = match.group(0)

            if value.startswith('"') and value.endswith('"'):
                token_type = "PHRASE"

            elif value.upper() == "AND":
                token_type = "AND"

            elif value.upper() == "OR":
                token_type = "OR"

            elif value.upper() == "NOT":
                token_type = "NOT"

            elif value == "(":
                token_type = "LPAREN"

            elif value == ")":
                token_type = "RPAREN"

            else:
                token_type = "TERM"

            tokens.append(
                QueryToken(
                    value=value,
                    token_type=token_type,
                    position=position,
                )
            )

        return tokens
