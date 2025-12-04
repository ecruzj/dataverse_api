import os
import base64
from typing import List, Dict, Optional
from datetime import datetime

from ..core.services.dataverse_client import call_dataverse

MAX_PAGE = 500
PREFER_HEADERS = {"Prefer": f"odata.maxpagesize={MAX_PAGE}"}

def _safe_filename(name: str) -> str:
    bad = '<>:"/\\|?*'
    for ch in bad:
        name = name.replace(ch, "_")
    return name.strip() or "file"

def _write_file(path: str, content_b64: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(base64.b64decode(content_b64))

class TimelineAttachmentsService:
    """
    Descarga adjuntos presentes en el Timeline de un registro de Dataverse:
    - Notas (annotations) con archivo (isdocument = true)
    - Adjuntos de correos (activitymimeattachments) de emails 'regarding' el registro
    """
    def __init__(self, out_dir: str = "./downloads/timeline"):
        self.out_dir = out_dir

    # ---------- PUBLIC API ----------
    def download_for_record(self, record_id: str, subfolder_name: Optional[str] = None) -> Dict[str, int]:
        """
        record_id: GUID del registro (Account, Incident, etc.)
        subfolder_name: nombre opcional para la carpeta (si no, usa el GUID)
        """
        folder = os.path.join(self.out_dir, subfolder_name or record_id)
        counts = {"notes": 0, "emails": 0}

        # 1) Notas con archivo
        notes = self._fetch_note_attachments(record_id)
        for n in notes:
            fn = _safe_filename(n.get("filename") or f"note_{n['annotationid']}.bin")
            path = os.path.join(folder, "notes", fn)
            body = n.get("documentbody")
            if body:
                _write_file(path, body)
                counts["notes"] += 1

        # 2) Adjuntos de correos whose regarding == record_id
        email_map = self._fetch_emails(record_id)  # {emailid: subject}
        for email_id, subject in email_map.items():
            atts = self._fetch_email_attachments(email_id)
            for att in atts:
                fn = _safe_filename(att.get("filename") or f"emailatt_{att['activitymimeattachmentid']}.bin")
                subject_part = _safe_filename(subject)[:80] if subject else "no_subject"
                path = os.path.join(folder, "emails", f"{subject_part}", fn)
                body = att.get("body")
                if body:
                    _write_file(path, body)
                    counts["emails"] += 1

        return counts

    # ---------- NOTES (Annotations) ----------
    def _fetch_note_attachments(self, record_id: str) -> List[Dict]:
        # Nota: _objectid_value es el regarding del annotation
        endpoint = (
            "annotations"
            f"?$filter=isdocument eq true and _objectid_value eq {record_id}"
            "&$select=annotationid,filename,mimetype,documentbody,filesize,subject,createdon"
            f"&$top={MAX_PAGE}"
        )
        data = call_dataverse(endpoint, method="GET", headers_extra=PREFER_HEADERS)
        return data.get("value", [])

    # ---------- EMAILS ----------
    def _fetch_emails(self, record_id: str) -> Dict[str, str]:
        # Emails cuyo regarding es el registro
        endpoint = (
            "emails"
            f"?$filter=_regardingobjectid_value eq {record_id}"
            "&$select=activityid,subject,createdon"
            f"&$orderby=createdon desc"
            f"&$top={MAX_PAGE}"
        )
        data = call_dataverse(endpoint, method="GET", headers_extra=PREFER_HEADERS)
        emails = {}
        for row in data.get("value", []):
            emails[row["activityid"]] = row.get("subject") or ""
        return emails

    def _fetch_email_attachments(self, email_activity_id: str) -> List[Dict]:
        # Adjuntos del email: body (base64), filename, mimetype, filesize
        endpoint = (
            "activitymimeattachments"
            f"?$filter=_objectid_value eq {email_activity_id}"
            "&$select=activitymimeattachmentid,filename,mimetype,body,filesize"
            f"&$top={MAX_PAGE}"
        )
        data = call_dataverse(endpoint, method="GET", headers_extra=PREFER_HEADERS)
        return data.get("value", [])