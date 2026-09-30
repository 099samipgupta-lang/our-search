from __future__ import annotations

import ast
import math
import operator
import re
from dataclasses import dataclass
from typing import Dict


@dataclass
class MathResult:
    expression: str
    answer: str
    success: bool
    error: str = ""


class MathCapabilityEngine:
    VERSION = "math-capability.v1"

    _operators = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.Pow: operator.pow,
        ast.Mod: operator.mod,
        ast.FloorDiv: operator.floordiv,
    }

    _unary = {
        ast.UAdd: operator.pos,
        ast.USub: operator.neg,
    }

    _functions = {
        "sqrt": math.sqrt,
        "sin": math.sin,
        "cos": math.cos,
        "tan": math.tan,
        "log": math.log,
        "log10": math.log10,
        "exp": math.exp,
        "abs": abs,
        "ceil": math.ceil,
        "floor": math.floor,
    }

    _constants = {
        "pi": math.pi,
        "e": math.e,
    }

    def _evaluate(self, node: ast.AST) -> float:
        if isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float)):
                return node.value
            raise ValueError("Unsupported constant.")

        if isinstance(node, ast.BinOp):
            operation = self._operators.get(
                type(node.op)
            )

            if operation is None:
                raise ValueError(
                    "Unsupported operator."
                )

            left = self._evaluate(node.left)
            right = self._evaluate(node.right)

            return operation(left, right)

        if isinstance(node, ast.UnaryOp):
            operation = self._unary.get(
                type(node.op)
            )

            if operation is None:
                raise ValueError(
                    "Unsupported unary operator."
                )

            return operation(
                self._evaluate(node.operand)
            )

        if isinstance(node, ast.Name):
            if node.id in self._constants:
                return self._constants[node.id]

            raise ValueError(
                f"Unknown constant: {node.id}"
            )

        if isinstance(node, ast.Call):
            if not isinstance(
                node.func,
                ast.Name,
            ):
                raise ValueError(
                    "Unsupported function."
                )

            function = self._functions.get(
                node.func.id
            )

            if function is None:
                raise ValueError(
                    f"Unknown function: {node.func.id}"
                )

            arguments = [
                self._evaluate(argument)
                for argument in node.args
            ]

            return function(*arguments)

        raise ValueError(
            "Unsupported mathematical expression."
        )

    def calculate(
        self,
        expression: str,
    ) -> MathResult:
        expression = expression.strip()

        if not expression:
            return MathResult(
                expression=expression,
                answer="",
                success=False,
                error="Empty expression.",
            )

        try:
            tree = ast.parse(
                expression,
                mode="eval",
            )

            value = self._evaluate(tree.body)

            if isinstance(value, float):
                if not math.isfinite(value):
                    raise ValueError(
                        "Result is not finite."
                    )

                if value.is_integer():
                    answer = str(int(value))
                else:
                    answer = str(value)
            else:
                answer = str(value)

            return MathResult(
                expression=expression,
                answer=answer,
                success=True,
            )

        except Exception as exc:
            return MathResult(
                expression=expression,
                answer="",
                success=False,
                error=str(exc),
            )

    def can_handle(
        self,
        query: str,
    ) -> bool:
        text = query.lower().strip()

        if not text:
            return False

        if any(
            phrase in text
            for phrase in (
                "calculate",
                "what is",
                "solve",
                "evaluate",
            )
        ):
            return bool(
                re.search(
                    r"\d",
                    text,
                )
            )

        return bool(
            re.fullmatch(
                r"[0-9+\-*/().%\s^]+",
                text,
            )
        )
