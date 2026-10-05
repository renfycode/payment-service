from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Экспоненциальные повторы: задержка перед повтором n равна base_delay * 2 ** (n - 1).

    max_attempts — общее число попыток, включая первую.
    """

    max_attempts: int
    base_delay: float

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay <= 0:
            raise ValueError("base_delay must be > 0")

    @property
    def delays_ms(self) -> tuple[int, ...]:
        """Задержки перед каждым повтором в миллисекундах (для TTL очередей)."""
        return tuple(
            round(self.base_delay * 2**retry * 1000) for retry in range(self.max_attempts - 1)
        )

    def delay_after(self, attempt: int) -> int | None:
        """Задержка (мс) перед следующей попыткой или None, если попытки исчерпаны."""
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        if attempt >= self.max_attempts:
            return None
        return self.delays_ms[attempt - 1]
