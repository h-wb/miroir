"""The product's base errors. The interface and the CLI show their message."""


class OperationError(RuntimeError):
    """An operation cannot be registered, adopted, or started."""


class Unavailable(OperationError):
    """What the operation needs cannot answer now; retrying later may work."""


class Timeout(OperationError):
    """A call reached its time limit."""
