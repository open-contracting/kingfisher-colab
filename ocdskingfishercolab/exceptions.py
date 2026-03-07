"""Exception classes."""


class OCDSKingfisherColabError(Exception):
    """Base class for exceptions from within this package."""


class UnknownPackageTypeError(OCDSKingfisherColabError, ValueError):
    """Raised when the provided package type is unknown."""


class MissingFieldsError(OCDSKingfisherColabError):
    """Raised when no fields are provided to a function."""
