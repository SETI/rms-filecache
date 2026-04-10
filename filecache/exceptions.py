##########################################################################################
# filecache/exceptions.py
##########################################################################################

from __future__ import annotations


class FileCacheError(Exception):
    """Base class for all filecache-specific exceptions."""
    pass


class UploadFailed(FileCacheError):
    """Raised when a file upload to a remote storage location fails.

    This exception wraps cloud-provider-specific exceptions (such as
    ``boto3.exceptions.S3UploadFailedError`` or
    ``google.api_core.exceptions.BadRequest``) so that callers do not need to
    import or understand the underlying cloud SDK in order to handle upload
    failures.
    """
    pass
