class LargeObjectPolicy:

    def __init__(
        self,
        threshold=4 * 1024 * 1024,
    ):
        if threshold <= 0:
            raise ValueError(
                "threshold must be greater than zero"
            )

        self.threshold = int(threshold)

    def is_large(self, data):
        if not isinstance(data, bytes):
            raise TypeError(
                "data must be bytes"
            )

        return len(data) >= self.threshold
