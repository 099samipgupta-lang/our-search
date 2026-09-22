class ChunkStore:

    DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024

    def __init__(
        self,
        storage,
        chunk_size=DEFAULT_CHUNK_SIZE,
    ):
        if chunk_size <= 0:
            raise ValueError(
                "chunk_size must be greater than zero"
            )

        self.storage = storage
        self.chunk_size = int(chunk_size)

    @staticmethod
    def _chunk_key(key, index):
        if not isinstance(key, str) or not key:
            raise ValueError(
                "key must be a non-empty string"
            )

        if not isinstance(index, int) or index < 0:
            raise ValueError(
                "chunk index must be a non-negative integer"
            )

        return f"{key}.chunks/{index:08d}"

    def chunk_count(self, data):
        if not isinstance(data, bytes):
            raise TypeError(
                "data must be bytes"
            )

        if not data:
            return 0

        return (
            len(data) + self.chunk_size - 1
        ) // self.chunk_size

    def split(self, data):
        if not isinstance(data, bytes):
            raise TypeError(
                "data must be bytes"
            )

        return [
            data[offset:offset + self.chunk_size]
            for offset in range(
                0,
                len(data),
                self.chunk_size,
            )
        ]

    def write_chunks(self, key, data):
        chunks = self.split(data)

        for index, chunk in enumerate(chunks):
            self.storage.put(
                self._chunk_key(key, index),
                chunk,
            )

        return len(chunks)

    def read_chunks(self, key, count):
        if not isinstance(count, int) or count < 0:
            raise ValueError(
                "chunk count must be a non-negative integer"
            )

        chunks = []

        for index in range(count):
            chunk = self.storage.get(
                self._chunk_key(key, index)
            )

            if chunk is None:
                raise ValueError(
                    f"missing chunk: {key}:{index}"
                )

            chunks.append(chunk)

        return chunks

    def read(self, key, count):
        return b"".join(
            self.read_chunks(
                key,
                count,
            )
        )

    def delete_chunks(self, key, count):
        deleted = 0

        for index in range(count):
            if self.storage.delete(
                self._chunk_key(key, index)
            ):
                deleted += 1

        return deleted
