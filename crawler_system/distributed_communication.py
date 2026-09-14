from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from typing import Callable, Optional

from crawler_system.communication_protocol import (
    CommunicationMessage,
    CommunicationProtocol,
    CommunicationProtocolError,
)
from crawler_system.communication_transport import (
    CommunicationTransportError,
    TCPTransport,
)
from crawler_system.message_delivery import (
    MessageDeliveryError,
    MessageDeliveryTracker,
)
from crawler_system.result import CrawlResult
from crawler_system.task import CrawlTask


class DistributedCommunicationError(Exception):
    """Raised for distributed communication integration failures."""


@dataclass(frozen=True)
class DistributedTaskDispatch:
    message_id: str
    task: CrawlTask
    sender_node_id: str
    receiver_node_id: str
    sent_at: float


@dataclass(frozen=True)
class DistributedTaskResult:
    message_id: str
    task_id: str
    result: CrawlResult
    sender_node_id: str
    receiver_node_id: str
    received_at: float


class DistributedCommunicationNode:
    """
    Real node-to-node integration for distributed crawling.

    Flow:

        DistributedDispatcher
                ↓
        CommunicationProtocol
                ↓
            TCPTransport
                ↓
        MessageDeliveryTracker
                ↓
          remote node
                ↓
            task executor
                ↓
           CrawlResult
                ↓
          source node
    """

    def __init__(
        self,
        node_id: str,
        cluster_id: str,
        transport: TCPTransport,
        delivery: MessageDeliveryTracker,
        dispatcher=None,
        task_executor: Optional[Callable[[CrawlTask], CrawlResult]] = None,
        peer_endpoints: Optional[dict[str, tuple[str, int]]] = None,
    ):
        if not node_id:
            raise ValueError("node_id must not be empty")

        if not cluster_id:
            raise ValueError("cluster_id must not be empty")

        self.node_id = node_id
        self.cluster_id = cluster_id
        self.transport = transport
        self.delivery = delivery
        self.dispatcher = dispatcher
        self.task_executor = task_executor
        self.peer_endpoints = dict(peer_endpoints or {})

        self.protocol = CommunicationProtocol(cluster_id)

        self._lock = threading.RLock()
        self._started = False

        self._processed_messages: set[str] = set()
        self._processed_tasks: dict[str, CrawlResult] = {}

        self._dispatches: dict[str, DistributedTaskDispatch] = {}
        self._results: dict[str, DistributedTaskResult] = {}
        self._errors: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        with self._lock:
            if self._started:
                return

            self.transport.start_server(self._handle_message)
            self._started = True

    def stop(self) -> None:
        with self._lock:
            if not self._started:
                return

            self.transport.stop()
            self._started = False

    def running(self) -> bool:
        with self._lock:
            return self._started and self.transport.running()

    # ------------------------------------------------------------------
    # Peer configuration
    # ------------------------------------------------------------------

    def set_peer_endpoint(
        self,
        node_id: str,
        host: str,
        port: int,
    ) -> None:
        if not node_id:
            raise ValueError("peer node_id must not be empty")

        if not host:
            raise ValueError("peer host must not be empty")

        if not 1 <= port <= 65535:
            raise ValueError("peer port must be between 1 and 65535")

        with self._lock:
            self.peer_endpoints[node_id] = (host, port)

    def peer_endpoint(
        self,
        node_id: str,
    ) -> tuple[str, int]:
        with self._lock:
            endpoint = self.peer_endpoints.get(node_id)

        if endpoint is None:
            raise DistributedCommunicationError(
                f"no endpoint configured for node: {node_id}"
            )

        return endpoint

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_message(
        self,
        message: CommunicationMessage,
    ) -> None:
        if message.cluster_id != self.cluster_id:
            raise DistributedCommunicationError(
                "message belongs to a different cluster"
            )

        if message.receiver_node_id != self.node_id:
            raise DistributedCommunicationError(
                "message addressed to a different node"
            )

    # ------------------------------------------------------------------
    # ACK
    # ------------------------------------------------------------------

    def _acknowledge(
        self,
        message: CommunicationMessage,
    ) -> CommunicationMessage:
        return self.protocol.acknowledgement(
            sender_node_id=self.node_id,
            receiver_node_id=message.sender_node_id,
            acknowledged_message_id=message.message_id,
        )

    # ------------------------------------------------------------------
    # Task execution
    # ------------------------------------------------------------------

    def _execute_task(
        self,
        task: CrawlTask,
    ) -> CrawlResult:
        if self.task_executor is None:
            raise DistributedCommunicationError(
                "no task executor configured"
            )

        result = self.task_executor(task)

        if not isinstance(result, CrawlResult):
            raise DistributedCommunicationError(
                "task executor must return CrawlResult"
            )

        if result.task.document_id != task.document_id:
            raise DistributedCommunicationError(
                "task executor returned a result for a different task"
            )

        if result.task.url != task.url:
            raise DistributedCommunicationError(
                "task executor returned a result for a different URL"
            )

        return result

    # ------------------------------------------------------------------
    # Result delivery
    # ------------------------------------------------------------------

    def _send_result_to_peer(
        self,
        result: CrawlResult,
        receiver_node_id: str,
        message_id: str,
    ) -> None:
        host, port = self.peer_endpoint(receiver_node_id)

        result_message = self.protocol.task_result(
            result=result,
            sender_node_id=self.node_id,
            receiver_node_id=receiver_node_id,
            message_id=message_id,
        )

        transport = TCPTransport(
            host=host,
            port=port,
            max_message_bytes=self.transport.max_message_bytes,
            connect_timeout=self.transport.connect_timeout,
            io_timeout=self.transport.io_timeout,
            cluster_id=self.cluster_id,
        )

        transport.send_message(result_message)

    # ------------------------------------------------------------------
    # Incoming TASK_DISPATCH
    # ------------------------------------------------------------------

    def _process_received_task(
        self,
        message: CommunicationMessage,
        task: CrawlTask,
    ) -> None:
        try:
            with self._lock:
                result = self._processed_tasks.get(task.document_id)

            if result is None:
                result = self._execute_task(task)

                with self._lock:
                    self._processed_tasks[task.document_id] = result

            self._send_result_to_peer(
                result=result,
                receiver_node_id=message.sender_node_id,
                message_id=message.message_id,
            )

        except Exception as error:
            with self._lock:
                self._errors[message.message_id] = str(error)

            try:
                host, port = self.peer_endpoint(
                    message.sender_node_id
                )

                error_message = self.protocol.error(
                    sender_node_id=self.node_id,
                    receiver_node_id=message.sender_node_id,
                    error=str(error),
                    failed_message_id=message.message_id,
                )

                TCPTransport(
                    host=host,
                    port=port,
                    max_message_bytes=self.transport.max_message_bytes,
                    connect_timeout=self.transport.connect_timeout,
                    io_timeout=self.transport.io_timeout,
                    cluster_id=self.cluster_id,
                ).send_message(error_message)

            except Exception:
                pass

    def _handle_task_dispatch(
        self,
        message: CommunicationMessage,
        address,
    ) -> CommunicationMessage:
        self._validate_message(message)

        task = self.protocol.extract_task(message)

        with self._lock:
            already_processed = message.message_id in self._processed_messages

            if not already_processed:
                self._processed_messages.add(message.message_id)

        if not already_processed:
            with self._lock:
                self._dispatches[message.message_id] = (
                    DistributedTaskDispatch(
                        message_id=message.message_id,
                        task=task,
                        sender_node_id=message.sender_node_id,
                        receiver_node_id=self.node_id,
                        sent_at=time.time(),
                    )
                )

            thread = threading.Thread(
                target=self._process_received_task,
                args=(message, task),
                daemon=True,
            )
            thread.start()

        # ACK means the remote node accepted the dispatch.
        # Task execution and TASK_RESULT happen asynchronously.
        return self._acknowledge(message)

    # ------------------------------------------------------------------
    # Incoming TASK_RESULT
    # ------------------------------------------------------------------

    def _handle_task_result(
        self,
        message: CommunicationMessage,
        address,
    ) -> None:
        self._validate_message(message)

        result = self.protocol.extract_result(message)

        with self._lock:
            if message.message_id in self._results:
                return

            self._results[message.message_id] = DistributedTaskResult(
                message_id=message.message_id,
                task_id=result.task.document_id,
                result=result,
                sender_node_id=message.sender_node_id,
                receiver_node_id=self.node_id,
                received_at=time.time(),
            )

        # The result message uses the original dispatch ID.
        # Complete the delivery lifecycle only after the result arrives.
        try:
            if not self.delivery.is_acknowledged(message.message_id):
                self.delivery.acknowledge(message.message_id)

            self.delivery.complete(message.message_id)

        except MessageDeliveryError:
            # The source may already have completed the delivery through
            # another valid path. Do not corrupt the received result.
            pass

        if self.dispatcher is not None:
            task_id = result.task.document_id

            if self.dispatcher.is_in_flight(task_id):
                self.dispatcher.complete(task_id)

    # ------------------------------------------------------------------
    # Incoming ACK
    # ------------------------------------------------------------------

    def _handle_ack(
        self,
        message: CommunicationMessage,
    ) -> None:
        self._validate_message(message)

        acknowledged_message_id = message.payload.get(
            "acknowledged_message_id"
        )

        if not acknowledged_message_id:
            raise CommunicationProtocolError(
                "ACK missing acknowledged_message_id"
            )

        self.delivery.acknowledge(
            acknowledged_message_id
        )

    # ------------------------------------------------------------------
    # Incoming ERROR
    # ------------------------------------------------------------------

    def _handle_error(
        self,
        message: CommunicationMessage,
    ) -> None:
        self._validate_message(message)

        failed_message_id = message.payload.get(
            "failed_message_id"
        )

        error = message.payload.get(
            "error",
            "remote communication error",
        )

        if failed_message_id:
            with self._lock:
                self._errors[failed_message_id] = str(error)

    # ------------------------------------------------------------------
    # Main message router
    # ------------------------------------------------------------------

    def _handle_message(
        self,
        message: CommunicationMessage,
        address,
    ):
        if message.message_type == CommunicationProtocol.TASK_DISPATCH:
            return self._handle_task_dispatch(
                message,
                address,
            )

        if message.message_type == CommunicationProtocol.TASK_RESULT:
            self._handle_task_result(
                message,
                address,
            )
            return None

        if message.message_type == CommunicationProtocol.ACK:
            self._handle_ack(message)
            return None

        if message.message_type == CommunicationProtocol.ERROR:
            self._handle_error(message)
            return None

        raise CommunicationProtocolError(
            f"unsupported message type: {message.message_type}"
        )

    # ------------------------------------------------------------------
    # Outgoing dispatch
    # ------------------------------------------------------------------

    def dispatch(
        self,
        task: CrawlTask,
        receiver_host: str,
        receiver_port: int,
        message_id: Optional[str] = None,
    ) -> DistributedTaskDispatch:
        if self.dispatcher is None:
            raise DistributedCommunicationError(
                "dispatcher is required for dispatch()"
            )

        if not self.running():
            raise DistributedCommunicationError(
                "source node transport is not running"
            )

        decision = self.dispatcher.dispatch(task)

        message_id = message_id or uuid.uuid4().hex

        message = self.protocol.task_dispatch(
            task=task,
            sender_node_id=self.node_id,
            receiver_node_id=decision.node_id,
            message_id=message_id,
        )

        self.delivery.register(
            message_id=message_id,
            sender_node_id=self.node_id,
            receiver_node_id=decision.node_id,
        )

        try:
            response = TCPTransport(
                host=receiver_host,
                port=receiver_port,
                max_message_bytes=self.transport.max_message_bytes,
                connect_timeout=self.transport.connect_timeout,
                io_timeout=self.transport.io_timeout,
                cluster_id=self.cluster_id,
            ).request(message)

            if response.message_type == CommunicationProtocol.ACK:
                self._handle_ack(response)

            elif response.message_type == CommunicationProtocol.ERROR:
                self._handle_error(response)

                self.dispatcher.release(
                    task.document_id
                )

                raise DistributedCommunicationError(
                    response.payload.get(
                        "error",
                        "remote node rejected task",
                    )
                )

            else:
                self.dispatcher.release(
                    task.document_id
                )

                raise DistributedCommunicationError(
                    "unexpected response to task dispatch"
                )

        except Exception as error:
            if self.dispatcher.is_in_flight(task.document_id):
                self.dispatcher.release(task.document_id)

            raise DistributedCommunicationError(
                f"task dispatch failed: {error}"
            ) from error

        dispatch = DistributedTaskDispatch(
            message_id=message_id,
            task=task,
            sender_node_id=self.node_id,
            receiver_node_id=decision.node_id,
            sent_at=time.time(),
        )

        with self._lock:
            self._dispatches[message_id] = dispatch

        return dispatch

    # ------------------------------------------------------------------
    # State inspection
    # ------------------------------------------------------------------

    def dispatch_record(
        self,
        message_id: str,
    ):
        with self._lock:
            return self._dispatches.get(message_id)

    def result_record(
        self,
        message_id: str,
    ):
        with self._lock:
            return self._results.get(message_id)

    def processed_task_count(self) -> int:
        with self._lock:
            return len(self._processed_tasks)

    def dispatch_count(self) -> int:
        with self._lock:
            return len(self._dispatches)

    def result_count(self) -> int:
        with self._lock:
            return len(self._results)

    def errors(self) -> dict[str, str]:
        with self._lock:
            return dict(self._errors)

    def clear_completed(self) -> None:
        self.delivery.clear_completed()

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()
