import requests
from ..core.logging.logging_conf import get_logger
from ..core.services.dataverse_client import call_dataverse
from ..core.automation.sharepoint.sharepoint_downloader import download_from_sharepoint
from urllib.parse import quote

log = get_logger(__name__)

def get_documents_for_account(account_id):
    # Get Document Locations
    location_query = f"sharepointdocumentlocations?$filter=_regardingobjectid_value eq {account_id}"
    locations = call_dataverse(location_query)

    if not locations["value"]:
        print("No SharePoint document locations found.")
        log.info(f"No SharePoint document locations found for account ID: {account_id}")
        return []
    
    # for loc in locations["value"]:
    #     print("Relative URL:", loc.get("relativeurl"))
    
    # # Get the first location
    # location_id = locations["value"][0]["sharepointdocumentlocationid"]

    # # Get the documents for the location
    # docs_query = f"sharepointdocuments?$filter=locationid eq {location_id}"
    # documents = call_dataverse(docs_query)

    # return documents["value"]
    
    # recent_url = get_most_recent_relativeurl(locations["value"])
    
    return locations

def get_relativeurls_for_object_id(object_id):
    # Query SharePoint document locations associated with the given object ID
    location_query = f"sharepointdocumentlocations?$filter=_regardingobjectid_value eq {object_id}"
    response = call_dataverse(location_query)

    if not response.get("value"):
        print(f"No SharePoint document locations found for object ID: {object_id}")
        log.info(f"No SharePoint document locations found for object ID: {object_id}")
        return []

    # Extract all 'relativeurl' values
    relative_urls = [location.get("relativeurl") for location in response["value"] if location.get("relativeurl")]

    return relative_urls

def get_latest_location_for_object_id(object_id):
    # Get Document Locations
    location_query = f"sharepointdocumentlocations?$filter=_regardingobjectid_value eq {object_id}"
    locations = call_dataverse(location_query)

    if not locations["value"]:
        print("No SharePoint document locations found.")
        log.info(f"No SharePoint document locations found for object ID: {object_id}")
        return []
    
    lastest_relativeurl = get_most_recent_relativeurl(locations["value"])
    
    return lastest_relativeurl

def get_most_recent_relativeurl(locations):
    locations_sorted = sorted(
        locations,
        key=lambda loc: loc.get("modifiedon", ""),
        reverse=True
    )
    return locations_sorted[0]["relativeurl"] if locations_sorted else None


def build_sharepoint_folder_url(relativeurl: str, entity_type: str):
    sharepoint_base_url = "https://ontariogov.sharepoint.com"
    sharepoint_site_path = "/sites/MGCS-CSOD/ICPS/"

    # Make sure to encode spaces and special characters
    encoded_relativeurl = quote(relativeurl.strip())

    # If it starts with "e-", use icps_ecase folder, otherwise use entity_type
    if relativeurl.startswith("e-"):
        full_url = f"{sharepoint_base_url}{sharepoint_site_path}icps_ecase/{encoded_relativeurl}"
    else:
        full_url = f"{sharepoint_base_url}{sharepoint_site_path}{entity_type}/{encoded_relativeurl}"
    
    return full_url

def list_files_sharepoint_rest(relative_url, access_token):
    base_site = "https://ontariogov.sharepoint.com/sites/MGCS-CSOD"
    folder_path = f"/sites/MGCS-CSOD/ICPS/account/{relative_url}"
    
    url = f"{base_site}/_api/web/GetFolderByServerRelativeUrl('{folder_path}')/Files"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json;odata=verbose"
    }
    
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        print(response.text)
        return []

    data = response.json()
    results = data.get("d", {}).get("results", [])

    return [
        {
            "name": file["Name"],
            "url": file["ServerRelativeUrl"],
            "timeLastModified": file["TimeLastModified"],
            "length": file["Length"]
        }
        for file in results
    ]