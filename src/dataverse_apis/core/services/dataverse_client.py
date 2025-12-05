import os
import requests
from ..auth.msal_auth import get_access_token_with_msal_default
from ..auth.msal_auth import get_access_token_with_msal_default
from dotenv import load_dotenv

# Environment variables
# ICPS URL
api_url = os.getenv("DATAVERSE_BASE_URI")
api_version = os.getenv("API_VERSION")
webapi_url = f"{api_url}/api/data/v{api_version}"


# def call_dataverse(endpoint: str, method: str = "GET", data: dict = None):
#     token = get_access_token_with_msal_default() #using the default methodL of MSAL
#     headers = {
#         "Authorization": f"Bearer {token}",
#         "Accept": "application/json",
#         "Content-Type": "application/json",
#         "OData-MaxVersion": "4.0",
#         "OData-Version": "4.0"
#     }
    
#     url = f"{webapi_url}/{endpoint}"
#     response = requests.request(method.upper(), url, headers=headers, json=data)
#     response.raise_for_status()
#     return response.json() if response.content else {"status": "success"}

def call_dataverse(endpoint: str, method: str = "GET", data: dict = None, headers_extra: dict = None):
    """
    Makes a request to the specified Dataverse endpoint.

    Parameters:
    - endpoint: string (e.g., 'WhoAmI' or 'contacts')
    - method: 'GET', 'POST', 'PUT', 'DELETE'
    - data: dictionary with the body (for POST or PUT)
    - headers_extra: optional additional headers

    Returns:
    - dict with the JSON response if successful
    """
    access_token = get_access_token_with_msal_default() #using the default methodL of MSAL
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }

    # Allow additional headers if needed
    if headers_extra:
        headers.update(headers_extra)

    full_url = f"{webapi_url}/{endpoint}"

    # Mapping methods to request functions
    method = method.upper()
    response = None
    try:
        if method == "GET":
            response = requests.get(full_url, headers=headers)
        elif method == "POST":
            response = requests.post(full_url, headers=headers, json=data)
        elif method == "PUT":
            response = requests.put(full_url, headers=headers, json=data)
        elif method == "PATCH":
            response = requests.patch(full_url, headers=headers, json=data)
        elif method == "DELETE":
            response = requests.delete(full_url, headers=headers)
        else:
            raise ValueError(f"HTTP method not supported: {method}")
        
        response.raise_for_status()
        # return response.json() if response.content else {"status": "success", "code": response.status_code}
        if response.content:
            try:
                json_payload = response.json()
            except ValueError:
                json_payload = {"raw": response.text}
        else:
            json_payload = {}

        return {
            "status": "success",
            "status_code": response.status_code,
            **json_payload,
        }
    except requests.exceptions.HTTPError:
        if response is None:
            return {
                "status": "error",
                "status_code": None,
                "error": "Request failed before response was received",
                "details": None,
            }
        status = response.status_code
        try:
            error_payload = response.json()
        except ValueError:
            error_payload = response.text
            error_payload = response.text

        # --- SPECIAL CASE: EXPIRED TOKEN / 401---
        if status == 401:
            return {
                "status": "error",
                "status_code": status,
                "error": "401 Unauthorized: access token expired or invalid",
                "details": error_payload,
            }

        # Other HTTP errors
        return {
            "status": "error",
            "status_code": status,
            "error": f"HTTP {status}",
            "details": error_payload,
        }

    except Exception as e:
        # Unexpected errors (network, etc.)
        return {
            "status": "error",
            "status_code": None,
            "error": f"Unexpected error: {e}",
            "details": None,
        }