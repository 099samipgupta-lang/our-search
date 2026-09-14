import json
import socket
import struct
import threading
from typing import Optional

from crawler_system.communication_protocol import (
    CommunicationMessage,
    CommunicationProtocol,
    CommunicationProtocolError,
)


class CommunicationTransportError(Exception):
    """Raised when transport-level communication fails."""


class TCPTransport:
    """
    Length-prefixed TCP transport for CommunicationMessage objects.

    Frame format:
        4-byte unsigned big-endian payload length
        payload bytes
    """

    HEADER_SIZE = 4
    DEFAULT_MAX_MESSAGE_BYTES = 10 * 1024 * 1024
    DEFAULT_CONNECT_TIMEOUT = 10.0
    DEFAULT_IO_TIMEOUT = 10.0

    def __init__(
        self,
        host: str,
        port: int,
        max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        io_timeout: float = DEFAULT_IO_TIMEOUT,
        cluster_id: str = "our-search",
    ):
        if not host:
            raise ValueError("host must not be empty")

        if not (1 <= int(port) <= 65535):
            raise ValueError("port must be between 1 and 65535")

        if max_message_bytes <= 0:
            raise ValueError("max_message_bytes must be positive")

        if connect_timeout <= 0:
            raise ValueError("connect_timeout must be positive")

        if io_timeout <= 0:
            raise ValueError("io_timeout must be positive")

        self.host = host
        self.port = int(port)
        self.max_message_bytes = int(max_message_bytes)
        self.connect_timeout = float(connect_timeout)
        self.io_timeout = float(io_timeout)

        if not str(cluster_id).strip():
            raise ValueError("cluster_id must be non-empty")

        self.cluster_id = str(cluster_id).strip()
        self.protocol = CommunicationProtocol(self.cluster_id)

        self._server_socket: Optional[socket.socket] = None
        self._stop_event = threading.Event()
        self._connections = set()
        self._connections_lock = threading.RLock()

    @staticmethod
    def _encode_frame(payload: bytes, max_message_bytes: int) -> bytes:
        if not isinstance(payload, bytes):
            raise CommunicationTransportError(
                "payload must be bytes"
            )

        if len(payload) > max_message_bytes:
            raise CommunicationTransportError(
                f"message exceeds maximum size: "
                f"{len(payload)} > {max_message_bytes}"
            )

        return struct.pack(">I", len(payload)) + payload

    @staticmethod
    def _recv_exact(
        connection: socket.socket,
        size: int,
    ) -> bytes:
        chunks = []
        remaining = size

        while remaining:
            chunk = connection.recv(remaining)

            if not chunk:
                raise CommunicationTransportError(
                    "connection closed before complete frame was received"
                )

            chunks.append(chunk)
            remaining -= len(chunk)

        return b"".join(chunks)

    @classmethod
    def _decode_frame(
        cls,
        connection: socket.socket,
        max_message_bytes: int,
    ) -> bytes:
        header = cls._recv_exact(connection, cls.HEADER_SIZE)

        length = struct.unpack(">I", header)[0]

        if length > max_message_bytes:
            raise CommunicationTransportError(
                f"incoming message exceeds maximum size: "
                f"{length} > {max_message_bytes}"
            )

        return cls._recv_exact(connection, length)

    def send_message(
        self,
        message: CommunicationMessage,
    ) -> None:
        if self._stop_event.is_set():
            raise CommunicationTransportError(
                "transport is stopped"
            )

        payload = self.protocol.encode(message)

        frame = self._encode_frame(
            payload,
            self.max_message_bytes,
        )

        with socket.create_connection(
            (self.host, self.port),
            timeout=self.connect_timeout,
        ) as connection:
            connection.settimeout(self.io_timeout)
            connection.sendall(frame)

    def request(
        self,
        message: CommunicationMessage,
    ) -> CommunicationMessage:
        if self._stop_event.is_set():
            raise CommunicationTransportError(
                "transport is stopped"
            )

        payload = self.protocol.encode(message)

        frame = self._encode_frame(
            payload,
            self.max_message_bytes,
        )

        with socket.create_connection(
            (self.host, self.port),
            timeout=self.connect_timeout,
        ) as connection:
            connection.settimeout(self.io_timeout)
            connection.sendall(frame)

            response_payload = self._decode_frame(
                connection,
                self.max_message_bytes,
            )

        try:
            return self.protocol.decode(
                response_payload
            )
        except CommunicationProtocolError as exc:
            raise CommunicationTransportError(
                f"invalid protocol response: {exc}"
            ) from exc

    def start_server(self, handler) -> None:
        if self._server_socket is not None:
            raise CommunicationTransportError(
                "server is already running"
            )

        self._stop_event.clear()

        server = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM,
        )

        server.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1,
        )

        server.settimeout(0.5)

        try:
            server.bind((self.host, self.port))
            server.listen()
        except OSError as exc:
            server.close()
            raise CommunicationTransportError(
                f"failed to start TCP server: {exc}"
            ) from exc

        self._server_socket = server

        accept_thread = threading.Thread(
            target=self._accept_loop,
            args=(handler,),
            daemon=True,
            name=f"tcp-transport-{self.port}",
        )

        accept_thread.start()

    def _accept_loop(self, handler) -> None:
        server = self._server_socket

        while not self._stop_event.is_set():
            try:
                connection, address = server.accept()
            except socket.timeout:
                continue
            except OSError:
                if self._stop_event.is_set():
                    break
                continue

            with self._connections_lock:
                self._connections.add(connection)

            thread = threading.Thread(
                target=self._connection_loop,
                args=(connection, address, handler),
                daemon=True,
                name=f"tcp-peer-{address[0]}:{address[1]}",
            )

            thread.start()

    def _connection_loop(
        self,
        connection: socket.socket,
        address,
        handler,
    ) -> None:
        try:
            connection.settimeout(self.io_timeout)

            payload = self._decode_frame(
                connection,
                self.max_message_bytes,
            )

            try:
                message = self.protocol.decode(
                    payload
                )
            except CommunicationProtocolError as exc:
                raise CommunicationTransportError(
                    f"invalid protocol message: {exc}"
                ) from exc

            response = handler(message, address)

            if response is not None:
                response_payload = self.protocol.encode(
                    response
                )

                frame = self._encode_frame(
                    response_payload,
                    self.max_message_bytes,
                )

                connection.sendall(frame)

        except (
            CommunicationTransportError,
            OSError,
            socket.timeout,
        ):
            pass

        finally:
            try:
                connection.close()
            finally:
                with self._connections_lock:
                    self._connections.discard(connection)

    def stop(self) -> None:
        self._stop_event.set()

        server = self._server_socket
        self._server_socket = None

        if server is not None:
            try:
                server.close()
            except OSError:
                pass

        with self._connections_lock:
            connections = list(self._connections)
            self._connections.clear()

        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

            try:
                connection.close()
            except OSError:
                pass

    def running(self) -> bool:
        return (
            self._server_socket is not None
            and not self._stop_event.is_set()
        )

    def connection_count(self) -> int:
        with self._connections_lock:
            return len(self._connections)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()
