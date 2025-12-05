from typing import Dict, Any

def validate_dataverse_error_message(
    result: Dict[str, Any],
    resp: Dict[str, Any],
    key: str,
) -> None:
    """
    Copy the Dataverse response into the output dictionary and
    propagate status_code and error to the root level so they can be
    used by higher layers (tasks, token helpers, etc.).

    - result[key]           -> full resp (for debugging)
    - result["status_code"] -> resp["status_code"] (if exists)
    - result["error"]       -> resp["error"] (if it exists and wasn't already set)
    """
    result[key] = resp

    status_code = resp.get("status_code")
    if status_code is not None:
        result["status_code"] = status_code

    error_msg = resp.get("error")
    if error_msg:
        if not result.get("error"):
            result["error"] = error_msg
