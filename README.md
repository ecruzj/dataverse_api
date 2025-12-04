# 📦 Dataverse APIs  
### Modular Python Toolkit for Automating Microsoft Dataverse Operations

`Dataverse_APIs` is a Python project designed to automate Microsoft Dataverse actions using secure OAuth2 authentication.  
The goal is to replicate what an end user can normally do inside Dataverse—queries, updates, merges, timeline actions, and more—but executed programmatically, safely, and in batch.

---

## 🚀 Project Architecture

The project is organized into three main layers:

### **1. `features/` – Entity Endpoints**
Each entity (Account, Timeline, SharePoint, Incidents, etc.) has its own folder.  
Inside each folder, you define the **pure functions** that call Dataverse endpoints:

- Request builders  
- CRUD operations  
- Timeline/attachment handling  
- Any direct communication with the Dataverse API  

_No business logic here — only endpoint-level functionality._

---

### **2. `tasks/` – Business Logic & Processing**
Tasks handle everything related to:

- Reading/writing DataFrames  
- Validations and pre-checks  
- Building argument objects for the `features` layer  
- Orchestrating multi-step flows  
- Batch processing (merge, update, deactivate, fix notes, etc.)

A task *calls* a feature function and prepares all data required to execute it.

---

### **3. `main.py` – Entry Point**
The main script imports tasks and orchestrates the full automation flow:

- Authenticate  
- Execute a task (merge accounts, fetch IDs, create timeline notes, etc.)  
- Export results  
- Log progress  

This is the layer intended for real usage.

---

## 🔐 Authentication

Authentication uses **OAuth2 client credentials** to obtain an access token (valid for ~1h 20m).  
Credentials are stored in `.env`.

Required environment variables:

```
DATAVERSE_CLIENT_ID=
DATAVERSE_CLIENT_SECRET=
DATAVERSE_TENANT_ID=
DATAVERSE_RESOURCE_URL=
DATAVERSE_SCOPE=
```

---

## 📁 Folder Structure (Simplified)

```
src/dataverse_apis/
│
├── core/
│   ├── auth/           
│   ├── automation/     
│   ├── logging/        
│   └── services/       
│
├── features/           
│   ├── account/
│   └── timeline/
│
├── data/               
│
├── tasks/              
│   ├── fetch_accounts.py
│   ├── merge_accounts.py
│   ├── incidents.py
│   ├── timeline_attachments_service.py
│   └── object_id_resolver.py
│
└── main.py             
```

---

## 🛠️ Quick Example

```python
from tasks.merge_accounts import merge_accounts_batch

if __name__ == "__main__":
    merge_accounts_batch(input_path="Merge_Accounts.xlsx")
```

---

## 📦 Installation

```
pip install -r requirements.txt
```

Copy `.env.sample` → `.env` and add your credentials.

---

## 📜 Purpose

This project is ideal for:

- Bulk updates  
- Merging duplicated records  
- Timeline automation  
- Cleaning inactive entities  
- Data quality workflows  
- Any Dataverse repetitive task that normally requires many manual steps

---

## ✨ Notes

This repository is actively being refactored into a cleaner modular API.  
The long‑term goal is to make each entity plug‑and‑play, with reusable tasks and endpoint definitions.

---

