import json
from datetime import date
from src.dataverse_apis.core.logging.logging_conf import setup_logging, get_logger
from src.dataverse_apis.tasks.fetch_accounts import main as fetch_accounts_task, fetch_accounts, fetch_accounts_from_ICPS
from src.dataverse_apis.tasks.sharepoint_documents import get_documents_for_account, build_sharepoint_folder_url, get_latest_location_for_object_id, get_relativeurls_for_object_id, list_files_sharepoint_rest, download_from_sharepoint
from src.dataverse_apis.tasks.merge_accounts import process_merge_for_all_groups, call_create_account
from src.dataverse_apis.core.services.dataverse_client import call_dataverse

from src.dataverse_apis.tasks.account.account_tasks import call_deactivate_accounts, call_reactivate_accounts

def main():
    setup_logging(app_name="dataverse_apis")  # Logging setup
    log = get_logger(__name__)
    log.info("Reactivation script started")
    
    results_df = call_reactivate_accounts(
        df="accounts_to_reactivate_ICPS.xlsx",  # ya con BUS ID + accountid
        bus_id_columns=["BUS ID"],
        # note_id_column="annotationid",  # si decides extraer esto antes
        output_path="reactivated_accounts_results.xlsx",
    )

    print("Done. Reactivation results saved.")
    print(results_df.head())
    ## DEACTIVATE ACCOUNTS START
    # today_str = date.today().strftime("%B %d, %Y")
    # reason = (
    #     f"This account was deactivated on <strong>{today_str}</strong> due to the absence of any associated entities."
    #     "The data on this account is considered outdated and no longer relevant for active operations"
    # )
    # results_df = call_deactivate_accounts(
    #     df="accounts_to_deactivate_ICPS_with_ids.xlsx",
    #     bus_id_columns=["BUS ID"],
    #     reason_text= reason,
    #     performed_by="<strong>Josue Cruz with Dataverse_APIs (Batch #3)</strong><br>"
    #         "Supervised by: <strong>Trung Quach</strong> and <strong>Leila Rigor</strong><br>",
    #     output_path="data/deactivated_accounts_results.xlsx",
    # )

    # print("Done. Results saved to data/deactivated_accounts_results.xlsx")
    # print(results_df.head())    
    ## DEACTIVATE ACCOUNTS END
    
    # accounts = fetch_accounts_from_ICPS()
    # print(f"Fetched {len(accounts)} accounts from ICPS.")
    
    #Create new account
    # resp = call_create_account(
    # name="1000240922 ONTARIO INC.",
    # accountnumber="",
    # phone="9058054356",
    # email="info@jollyzeealpacas.com",
    # website="https://jollyzeealpacas.com",
    # addr={"line1":"123 Farm Rd", "city":"Guelph", "state":"ON", "zip":"N1K 1A1", "country":"CA"},
    # # opcionales:
    # # parent_account_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    # # owner_user_id="ffffffff-1111-2222-3333-444444444444",
    # # extra={"accountcategorycode": 1}  # OptionSet
    # )
    
    # print("Created Account Response:", json.dumps(resp, indent=2))
    
    # Process the Merge for Accounts
    # print("Merge Accounts Start...")
    # df = fetch_accounts()
    # df = process_merge_for_all_groups(df)
    # print("Merge Accounts End...")
    
    # Account
    # account_number = "BUS-028989"
    # endpoint = f"accounts?$filter=accountnumber eq '{account_number}'"
    # result = call_dataverse(endpoint)    
    # print(json.dumps(result, indent=2))
    # key_property: accountid
        
    # CASE
    # ticket_number = "CAS-114810-J1R1L"
    # endpoint = f"incidents?$filter=ticketnumber eq '{ticket_number}'"
    # result = call_dataverse(endpoint)    
    # print(json.dumps(result, indent=2))
    # key_property: incidentid
    
    # eCase
    # icps_name = "e-054471"
    # endpoint = f"icps_ecases?$filter=icps_name eq '{icps_name}'"
    # result = call_dataverse(endpoint)    
    # print(json.dumps(result, indent=2))
    # key_property: icps_ecaseid
    
    # Inspection
    # icps_name = "INS-003208"
    # endpoint = f"icps_inspections?$filter=icps_name eq '{icps_name}'"
    # result = call_dataverse(endpoint)    
    # print(json.dumps(result, indent=2))
    # key_property: icps_inspectionid
    
    # Investigation
    # icps_name = "INV-000314"
    # endpoint = f"icps_investigations?$filter=icps_name eq '{icps_name}'"
    # result = call_dataverse(endpoint)    
    # print(json.dumps(result, indent=2))
    # key_property: icps_investigationid
        
    # get record by entity name and id
    # entity_name = "icps_inspections"
    # record_id = "3aafffdd-15d8-4466-b36a-a8763aa32d37"
    # url = f"{entity_name}({record_id})"
    # result = call_dataverse(url)    
    # print(json.dumps(result, indent=2))
    
    # endpoint = "sharepointdocumentlocations"
    # locations_result = call_dataverse(endpoint)
    # print(json.dumps(locations_result, indent=2))
    
    # record_id = "0e29994e-634c-f011-8779-6045bd5f62ba"

    # # # Primero obtén las ubicaciones de documentos asociadas a este Account
    # endpoint = f"sharepointdocumentlocations?$filter=_regardingobjectid_value eq {record_id}"
    # locations_result = call_dataverse(endpoint)
    # print(json.dumps(locations_result, indent=2))

    # # Get the documents for the location
    # location_id = "17d3d03f-7dbd-ef11-b8e8-6045bd60ca8a"
    # docs_query = f"sharepointdocuments?$filter=locationid eq {location_id}"
    # documents_result = call_dataverse(docs_query)
    # print(json.dumps(documents_result, indent=2))

    
    
    # print(json.dumps(incident_result, indent=2)) # print in JSON format
    
    # relative_url = "Kennedy Furniture_B91D0AE05F76ED1181AC0022483C554A"
    # folder_url = build_sharepoint_folder_url(relative_url, "account")
    # print("SharePoint folder URL:", folder_url)

    # object_id = "c8947c04-146f-4bf1-b1f0-ebc6cd5d3a56"
    # relative_urls = get_relativeurls_for_object_id(object_id)
    # print(json.dumps(relative_urls, indent=2))
    
    # for relative_url in relative_urls:
    #     sharepoint_url = build_sharepoint_folder_url(relative_url, "icps_investigation")
    #     print(f"sharepoint folder url: {sharepoint_url}")
    #     download_from_sharepoint(sharepoint_url, "INV-000286")
        
        
    # print(json.dumps(locations, indent=2)) # print in JSON format
    # fetch_accounts_task()
    
    # result = call_dataverse("WhoAmI")
    # print(f"WhoAmI Result: {result}")
    
    
    # this works for BUS-xxxxxx
    # account_number = "BUS-051760"
    # account_endpoint = f"accounts?$filter=accountnumber eq '{account_number}'"
    # account_result = call_dataverse(account_endpoint)
    
    # print(f"Account Id for {account_number}: {account_result["value"][0].get("accountid")}")
    
    # print(json.dumps(account_result, indent=2)) # print in JSON format
    # this works for BUS-xxxxxx
    
    # if account_result and "value" in account_result and len(account_result["value"]) > 0:
    #     account_id = account_result["value"][0].get("accountid")
    #     print(f"Account ID for {account_number}: {account_id}")
    # else:
    #     print(f"No account found with accountnumber: {account_number}")
    
    # result = call_dataverse("contacts", method="POST", data={
    #     "firstname": "Test",
    #     "lastname": "Contact",
    #     "emailaddress1": "test@example.com"
    # })
    # print("created contact:", result)
    
if __name__ == "__main__":
    main()