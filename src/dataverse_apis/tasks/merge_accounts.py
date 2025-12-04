import pandas as pd
import base64
import mimetypes
import os
from tqdm import tqdm
from .fetch_accounts import get_column_name
from ..core.services.dataverse_client import call_dataverse

OUTPUT_FILE = "data/merged_output_results.xlsx"

def call_merge_endpoint(target_account_id: str, subordinate_account_id: str) -> dict:
    try:
        endpoint = "Merge"
        payload = {
            "Target": {
                "@odata.type": "Microsoft.Dynamics.CRM.account",
                "accountid": target_account_id
            },
            "Subordinate": {
                "@odata.type": "Microsoft.Dynamics.CRM.account",
                "accountid": subordinate_account_id
            },
            "PerformParentingChecks": False
        }
        return call_dataverse(endpoint, method="POST", data=payload)
    except Exception as e:
        return {"status": f"error: {str(e)}", "code": 500}
    
def call_create_account_note(target_account_id: str, subject: str, body_text: str) -> dict:
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
    
def call_create_account_note_with_file(target_account_id: str, subject: str, body_text: str,
                                       file_path: str, mimetype: str | None = None) -> dict:
    """
    Upload a note with an attachment to the Account Timeline.
    """
    try:
        endpoint = "annotations"
        if not mimetype:
            mimetype = mimetypes.guess_type(file_path)[0] or "application/octet-stream"

        with open(file_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")

        payload = {
            "subject": subject,
            "notetext": body_text,
            "isdocument": True,
            "documentbody": b64,
            "filename": os.path.basename(file_path),
            "mimetype": mimetype,
            "objectid_account@odata.bind": f"/accounts({ _clean_guid(target_account_id) })"
        }
        return call_dataverse(endpoint, method="POST", data=payload)
    except Exception as e:
        return {"status": f"error: {str(e)}", "code": 500}

def call_create_account(
    name: str,
    accountnumber: str | None = None,
    phone: str | None = None,
    email: str | None = None,
    website: str | None = None,
    addr: dict | None = None,
    parent_account_id: str | None = None,
    owner_user_id: str | None = None,
    primary_contact_id: str | None = None,
    currency_id: str | None = None,
    extra: dict | None = None,   # para columns/custom fields adicionales
) -> dict:
    """
    Crea un Account y devuelve la entidad creada (si tu call_dataverse usa Prefer:return=representation)
    o al menos el OData-EntityId / status.
    """
    payload = {
        "name": name,
    }
    if accountnumber: payload["accountnumber"] = accountnumber
    if phone:         payload["telephone1"]   = phone
    if email:         payload["emailaddress1"]= email
    if website:       payload["websiteurl"]   = website

    # Dirección (si se provee)
    if addr:
        payload.update({
            "address1_line1":            addr.get("line1"),
            "address1_line2":            addr.get("line2"),
            "address1_city":             addr.get("city"),
            "address1_stateorprovince":  addr.get("state"),
            "address1_postalcode":       addr.get("zip"),
            "address1_country":          addr.get("country"),
        })

    # Lookups
    if parent_account_id:
        payload["parentaccountid@odata.bind"] = f"/accounts({_clean_guid(parent_account_id)})"
    if owner_user_id:
        payload["ownerid@odata.bind"] = f"/systemusers({_clean_guid(owner_user_id)})"
    if primary_contact_id:
        payload["primarycontactid@odata.bind"] = f"/contacts({_clean_guid(primary_contact_id)})"
    if currency_id:
        payload["transactioncurrencyid@odata.bind"] = f"/transactioncurrencies({_clean_guid(currency_id)})"

    # Campos extra (optionsets, custom columns, etc.)
    if extra:
        payload.update(extra)

    # Recomendado: pedir representación de vuelta para captar el accountid
    headers = {"Prefer": "return=representation"}

    return call_dataverse("accounts", method="POST", data=payload, headers_extra=headers)

def upsert_account_by_id(account_id: str, data: dict, create_if_missing: bool = True) -> dict:
    headers = {}
    if create_if_missing:
        headers["If-None-Match"] = "*"
    return call_dataverse(f"accounts({_clean_guid(account_id)})", method="PATCH", data=data, headers=headers)

def _clean_guid(g: str) -> str:
    """Remove braces and spaces from the GUID in case it comes with {}."""
    return g.strip().strip("{}")

def merge_accounts(target_account: dict, subordinate_accounts: list[dict]) -> dict:
    errors = []
    details = {}

    for subordinate in tqdm(
        subordinate_accounts,
        desc=f"🔃 Merging Group {target_account['Merge_Group_ID']}",
        unit="sub",
        leave=False,
        ncols=60
    ):
        subordinate_id = subordinate.get("accountid")
        if not subordinate_id:
            details["UNKNOWN"] = "❌ Subordinate without accountid"
            errors.append("Subordinate without accountid")
            continue

        try:
            result = call_merge_endpoint(target_account["accountid"], subordinate_id)
            code = result.get("code", None)
            status = result.get("status", "unknown")

            if code == 204:
                details[subordinate_id] = f"✅ Merge successful (code: {code})"
            else:
                msg = f"❌ Merge failed (code: {code}, status: {status})"
                details[subordinate_id] = msg
                errors.append(msg)

        except Exception as e:
            msg = f"❌ Exception: {str(e)}"
            details[subordinate_id] = msg
            errors.append(msg)

    summary = "✅ All merges successful" if not errors else "❌ Merge completed with errors"

    return {
        "summary": summary,
        "details": details
    }

def process_merge_for_all_groups(df: pd.DataFrame) -> pd.DataFrame:
    df["merge_result"] = None
    df["merge_detail"] = None
    column_name = get_column_name()

    duplicates = df[df.duplicated(column_name, keep=False)]
    if not duplicates.empty:
        raise Exception(f"❌ Error: Duplicates found in '{column_name}' column:\n{duplicates[[column_name, 'Merge_Group_ID']]}")

    for group_id, group in tqdm(df.groupby("Merge_Group_ID"), desc="🔄 Processing Merge Groups", unit="group"):
        target_row = group[group["Merge_Role"] == 1]
        subordinates = group[group["Merge_Role"] == 0]

        if len(target_row) == 0:
            df.loc[group.index, "merge_result"] = "❌ Error: No Target account in group"
            df.loc[group.index, "merge_detail"] = "No Target account present"
            continue
        elif len(target_row) > 1:
            df.loc[group.index, "merge_result"] = f"❌ Error: Multiple Target accounts in group ({len(target_row)})"
            df.loc[group.index, "merge_detail"] = "Multiple Target accounts detected"
            continue
        elif len(subordinates) == 0:
            df.loc[group.index, "merge_result"] = "❌ Error: No Subordinate in group"
            df.loc[group.index, "merge_detail"] = "No Subordinate present"
            continue

        target_account = target_row.iloc[0].to_dict()
        subordinate_accounts = subordinates.to_dict(orient="records")

        print("\n" + "-"*60)
        print(f"🔧 Merge Group: {group_id}")
        print(f"📌 Target Account ID: {target_account['accountid']}")
        print(f"   ↳ Subordinate(s): {[s['accountid'] for s in subordinate_accounts]}")

        merge_output = merge_accounts(target_account, subordinate_accounts)
        
        # Construir el texto de la nota con el resumen y los detalles por account
        target_id = target_account["accountid"]
        subject = f"Merge de Accounts - Group {group_id}"

        detail_lines = [
            f"Merge Account executed",
            f"• Target: {target_account['BUS ID']}",
            "• Subordinates: " + ", ".join([s["BUS ID"] for s in subordinate_accounts]),
            f"• Result: {merge_output['summary']}",
            "",
            "Details by Account:"
        ]
        # for acc_id, det in merge_output["details"].items():
        #     detail_lines.append(f"  - {acc_id}: {det}")
        
        for s in subordinate_accounts:
            acc_id = s["accountid"]
            bus_id = s.get("BUS ID") or acc_id
            det = merge_output["details"].get(acc_id, "Without detail")
            detail_lines.append(f"  - {bus_id}: {det}")

        note_text = "\n".join(detail_lines)

        # Crear la nota en el Timeline del Account padre
        note_resp = call_create_account_note(
            target_account_id=target_id,
            subject=subject,
            body_text=note_text
        )

        df.loc[group.index, "merge_result"] = merge_output["summary"]

        for idx, row in group.iterrows():
            account_id = row.get("accountid")
            detail = merge_output["details"].get(
                account_id,
                "✅ Main account (no action)" if row["Merge_Role"] == 1 else "⚠️ No detail"
            )
            df.at[idx, "merge_detail"] = detail

    df.to_excel(OUTPUT_FILE, index=False)
    print(f"\n📁 Generated file: {OUTPUT_FILE}")

    return df