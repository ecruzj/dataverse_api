from src.dataverse_apis.core.services.dataverse_client import call_dataverse
from typing import Optional

DEACTIVATION_SIGNATURE = "This account was deactivated on "

def create_account_note(target_account_id: str, subject: str, body_text: str) -> dict:
    """
    Create an annotation linked to an Account to appear on the Timeline.
    """
    try:
        endpoint = "annotations"  # Web API: POST /api/data/v9.2/annotations
        payload = {
            "subject": subject,
            "notetext": body_text,
            # Bind to the account target
            "objectid_account@odata.bind": f"/accounts({_clean_guid(target_account_id)})"
        }
        return call_dataverse(endpoint, method="POST", data=payload)
    except Exception as e:
        return {"status": f"error: {str(e)}", "code": 500}
    
def delete_note_by_id(note_id: str) -> dict:
    """
    Remove a specific annotation by its GUID.
    """
    try:
        endpoint = f"annotations({_clean_guid(note_id)})"
        return call_dataverse(endpoint, method="DELETE")
    except Exception as e:
        return {"status": f"error: {str(e)}", "code": 500}
    
def find_last_deactivation_note_for_account(account_id: str) -> Optional[str]:
    """
    Returns the annotationid of the last deactivation note created by our script.
    Uses a secure contains element based on a unique text signature.
    """

    endpoint = (
        "annotations?"
        "$select=annotationid,createdon,notetext&"
        "$orderby=createdon desc&"
        "$top=1&"
        "$filter="
        f"objectid_account/accountid eq {account_id} "
        f"and contains(notetext,'{DEACTIVATION_SIGNATURE}')"
    )

    try:
        resp = call_dataverse(endpoint, method="GET")
        value = resp.get("value", [])
        if not value:
            return None

        return value[0].get("annotationid")

    except Exception as e:
        return None
    
def _clean_guid(g: str) -> str:
    """Remove braces and spaces from the GUID in case it comes with {}."""
    return g.strip().strip("{}")