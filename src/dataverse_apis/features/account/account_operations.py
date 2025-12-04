# dataverse_apis/features/account/account_operations.py
from __future__ import annotations
from typing import Optional, Dict, Any
from src.dataverse_apis.core.services.dataverse_client import call_dataverse
from src.dataverse_apis.features.timeline.note_operations import create_account_note, delete_note_by_id, find_last_deactivation_note_for_account


def _clean_guid(g: str) -> str:
    """Remove braces and spaces from the GUID in case it comes with {}."""
    return g.strip().strip("{}")


def deactivate_account(account_id: str) -> dict:
    """
    Call the Dataverse Web API to deactivate an Account.

    CRM Standard:
    - statuscode: 1 (Inactive)
    - statuscode: 2 (Inactive)
    """
    endpoint = f"accounts({_clean_guid(account_id)})"
    payload = {
        "statecode": 1,  # Inactive
        "statuscode": 2,  # Inactive
    }

    # PATCH /accounts(<id>)
    return call_dataverse(endpoint, method="PATCH", data=payload)

def deactivate_account_with_note(
    account_id: str,
    reason: Optional[str] = None,
    performed_by: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Deactivate an account and create a note on the timeline explaining what happened.
    """
    result: Dict[str, Any] = {
        "account_id": account_id,
        "deactivated": False,
        "note_created": False,
        "error": None,
    }

    try:
        # 1) Deactivate your Dataverse account
        deactivate_resp = deactivate_account(account_id=account_id)
        result["deactivated"] = True
        result["deactivate_response"] = deactivate_resp

        # 2) Professional text formatting
        # -------------------------------
        reason_html = (
            f"{reason}<br>" if reason else ""
        )

        note_body = (
            f"{reason_html}"
            f"Action performed by: {performed_by}<br>"
        )

        # 3) Create note on the account timeline
        note_resp = create_account_note(
            target_account_id=account_id,
            subject="Account Deactivated",
            body_text=note_body,
        )
        result["note_created"] = True
        result["note_response"] = note_resp

    except Exception as exc:  # pylint: disable=broad-except
        result["error"] = str(exc)

    return result

def reactivate_account(account_id: str) -> dict:
    """
    Reactivate an Account (active statecode/statuscode).
    """
    endpoint = f"accounts({_clean_guid(account_id)})"
    payload = {
        "statecode": 0,  # Active
        "statuscode": 1,  # Active
    }

    return call_dataverse(endpoint, method="PATCH", data=payload)


def reactivate_account_and_delete_note(
    account_id: str,
    note_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Reactivate an account and optionally delete the deactivation note.

    - If note_id is None, only reactivate the account.
    - If it has a value, try deleting that annotation.
    """
    result: Dict[str, Any] = {
        "account_id": account_id,
        "reactivated": False,
        "note_deleted": False,
        "error": None,
    }

    try:
        # 1) Reactivate account
        reactivate_resp = reactivate_account(account_id)
        result["reactivated"] = True
        result["reactivate_response"] = reactivate_resp

        # 2) Delete note (if we have an ID)
        target_note_id = note_id
        if not target_note_id:
            # Automatically search for the deactivation note
            target_note_id = find_last_deactivation_note_for_account(
                _clean_guid(account_id)
            )

        if target_note_id:
            delete_resp = delete_note_by_id(target_note_id)
            result["note_deleted"] = True
            result["note_delete_response"] = delete_resp
        else:
            result["note_deleted"] = False  # Not found, it's not a fatal error

    except Exception as exc:
        result["error"] = str(exc)

    return result