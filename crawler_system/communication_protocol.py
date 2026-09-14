from __future__ import annotations

import base64
import json
import uuid
from dataclasses import dataclass
from typing import Any

from .result import CrawlResult
from .task import CrawlTask


class CommunicationProtocolError(ValueError):
    pass


def _encode_wire_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {
            "__wire_type__": "bytes",
            "encoding": "base64",
            "value": base64.b64encode(value).decode("ascii"),
        }

    if isinstance(value, dict):
        return {
            str(key): _encode_wire_value(item)
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [_encode_wire_value(item) for item in value]

    if isinstance(value, tuple):
        return [_encode_wire_value(item) for item in value]

    if isinstance(value, (str, int, float, bool)) or value is None:
        return value

    raise CommunicationProtocolError(
        f"unsupported wire value type: {type(value).__name__}"
    )


def _decode_wire_value(value: Any) -> Any:
    if isinstance(value, dict):
        if value.get("__wire_type__") == "bytes":
            if value.get("encoding") != "base64":
                raise CommunicationProtocolError(
                    "unsupported bytes encoding"
                )

            encoded = value.get("value")

            if not isinstance(encoded, str):
                raise CommunicationProtocolError(
                    "invalid encoded bytes payload"
                )

            try:
                return base64.b64decode(
                    encoded.encode("ascii"),
                    validate=True,
                )
            except (
                ValueError,
                UnicodeEncodeError,
                base64.binascii.Error,
            ) as error:
                raise CommunicationProtocolError(
                    "invalid base64 bytes payload"
                ) from error

        return {
            key: _decode_wire_value(item)
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [_decode_wire_value(item) for item in value]

    return value


@dataclass(frozen=True)
class CommunicationMessage:
    protocol_version: int
    cluster_id: str
    message_id: str
    message_type: str
    sender_node_id: str
    receiver_node_id: str
    payload: dict[str, Any]

    PROTOCOL_VERSION = 1

    def __post_init__(self):
        if int(self.protocol_version) != self.PROTOCOL_VERSION:
            raise CommunicationProtocolError(
                "unsupported protocol version"
            )

        for name, value in (
            ("cluster_id", self.cluster_id),
            ("message_id", self.message_id),
            ("message_type", self.message_type),
            ("sender_node_id", self.sender_node_id),
            ("receiver_node_id", self.receiver_node_id),
        ):
            if not isinstance(value, str) or not value.strip():
                raise CommunicationProtocolError(
                    f"{name} must be a non-empty string"
                )

        if not isinstance(self.payload, dict):
            raise CommunicationProtocolError(
                "payload must be a dictionary"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "protocol_version": self.protocol_version,
            "cluster_id": self.cluster_id,
            "message_id": self.message_id,
            "message_type": self.message_type,
            "sender_node_id": self.sender_node_id,
            "receiver_node_id": self.receiver_node_id,
            "payload": _encode_wire_value(self.payload),
        }

    def to_bytes(self) -> bytes:
        return json.dumps(
            self.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
    ) -> "CommunicationMessage":
        if not isinstance(data, dict):
            raise CommunicationProtocolError(
                "message must be a dictionary"
            )

        required = (
            "protocol_version",
            "cluster_id",
            "message_id",
            "message_type",
            "sender_node_id",
            "receiver_node_id",
            "payload",
        )

        for field in required:
            if field not in data:
                raise CommunicationProtocolError(
                    f"missing message field: {field}"
                )

        payload = _decode_wire_value(data["payload"])

        if not isinstance(payload, dict):
            raise CommunicationProtocolError(
                "decoded payload must be a dictionary"
            )

        return cls(
            protocol_version=int(data["protocol_version"]),
            cluster_id=str(data["cluster_id"]),
            message_id=str(data["message_id"]),
            message_type=str(data["message_type"]),
            sender_node_id=str(data["sender_node_id"]),
            receiver_node_id=str(data["receiver_node_id"]),
            payload=payload,
        )

    @classmethod
    def from_bytes(
        cls,
        data: bytes,
    ) -> "CommunicationMessage":
        if not isinstance(data, bytes):
            raise CommunicationProtocolError(
                "wire data must be bytes"
            )

        try:
            decoded = json.loads(data.decode("utf-8"))
        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as error:
            raise CommunicationProtocolError(
                "invalid wire message"
            ) from error

        return cls.from_dict(decoded)


class CommunicationProtocol:
    TASK_DISPATCH = "task_dispatch"
    TASK_RESULT = "task_result"
    ACK = "ack"
    ERROR = "error"

    MESSAGE_TYPES = frozenset({
        TASK_DISPATCH,
        TASK_RESULT,
        ACK,
        ERROR,
    })

    def __init__(self, cluster_id: str):
        cluster_id = str(cluster_id).strip()

        if not cluster_id:
            raise ValueError("cluster_id must be non-empty")

        self.cluster_id = cluster_id

    def _message(
        self,
        message_type: str,
        sender_node_id: str,
        receiver_node_id: str,
        payload: dict[str, Any],
        message_id: str | None = None,
    ) -> CommunicationMessage:
        if message_type not in self.MESSAGE_TYPES:
            raise CommunicationProtocolError(
                f"unknown message type: {message_type}"
            )

        return CommunicationMessage(
            protocol_version=CommunicationMessage.PROTOCOL_VERSION,
            cluster_id=self.cluster_id,
            message_id=message_id or uuid.uuid4().hex,
            message_type=message_type,
            sender_node_id=str(sender_node_id).strip(),
            receiver_node_id=str(receiver_node_id).strip(),
            payload=payload,
        )

    def task_dispatch(
        self,
        task: CrawlTask,
        sender_node_id: str,
        receiver_node_id: str,
        message_id: str | None = None,
    ) -> CommunicationMessage:
        if not isinstance(task, CrawlTask):
            raise TypeError("task must be CrawlTask")

        return self._message(
            self.TASK_DISPATCH,
            sender_node_id,
            receiver_node_id,
            {"task": task.to_dict()},
            message_id,
        )

    def task_result(
        self,
        result: CrawlResult,
        sender_node_id: str,
        receiver_node_id: str,
        message_id: str | None = None,
    ) -> CommunicationMessage:
        if not isinstance(result, CrawlResult):
            raise TypeError("result must be CrawlResult")

        return self._message(
            self.TASK_RESULT,
            sender_node_id,
            receiver_node_id,
            {"result": result.to_dict()},
            message_id,
        )

    def acknowledgement(
        self,
        sender_node_id: str,
        receiver_node_id: str,
        acknowledged_message_id: str,
    ) -> CommunicationMessage:
        return self._message(
            self.ACK,
            sender_node_id,
            receiver_node_id,
            {
                "acknowledged_message_id":
                    acknowledged_message_id
            },
        )

    def error(
        self,
        sender_node_id: str,
        receiver_node_id: str,
        error: str,
        failed_message_id: str | None = None,
    ) -> CommunicationMessage:
        return self._message(
            self.ERROR,
            sender_node_id,
            receiver_node_id,
            {
                "error": str(error),
                "failed_message_id": failed_message_id,
            },
        )

    def encode(
        self,
        message: CommunicationMessage,
    ) -> bytes:
        if not isinstance(message, CommunicationMessage):
            raise TypeError(
                "message must be CommunicationMessage"
            )

        if message.cluster_id != self.cluster_id:
            raise CommunicationProtocolError(
                "message belongs to a different cluster"
            )

        return message.to_bytes()

    def decode(
        self,
        data: bytes,
    ) -> CommunicationMessage:
        message = CommunicationMessage.from_bytes(data)

        if message.cluster_id != self.cluster_id:
            raise CommunicationProtocolError(
                "message belongs to a different cluster"
            )

        if message.message_type not in self.MESSAGE_TYPES:
            raise CommunicationProtocolError(
                f"unknown message type: {message.message_type}"
            )

        return message

    @staticmethod
    def extract_task(
        message: CommunicationMessage,
    ) -> CrawlTask:
        if message.message_type != CommunicationProtocol.TASK_DISPATCH:
            raise CommunicationProtocolError(
                "message is not a task dispatch"
            )

        try:
            return CrawlTask.from_dict(
                message.payload["task"]
            )
        except (
            KeyError,
            TypeError,
            ValueError,
        ) as error:
            raise CommunicationProtocolError(
                "invalid task dispatch payload"
            ) from error

    @staticmethod
    def extract_result(
        message: CommunicationMessage,
    ) -> CrawlResult:
        if message.message_type != CommunicationProtocol.TASK_RESULT:
            raise CommunicationProtocolError(
                "message is not a task result"
            )

        try:
            result_data = message.payload["result"]

            task = CrawlTask.from_dict(
                result_data["task"]
            )

            response = result_data["response"]

            if not isinstance(response, dict):
                raise CommunicationProtocolError(
                    "result response must be a dictionary"
                )

            return CrawlResult(
                task=task,
                response=response,
            )

        except (
            KeyError,
            TypeError,
            ValueError,
        ) as error:
            raise CommunicationProtocolError(
                "invalid task result payload"
            ) from error
