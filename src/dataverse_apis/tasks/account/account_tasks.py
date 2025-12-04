# dataverse_apis/tasks/account/account_tasks.py
from __future__ import annotations
from pathlib import Path
from datetime import datetime
from typing import Optional, Sequence, List, Dict, Any, Union

import pandas as pd
from tqdm.auto import tqdm
from rich.console import Console
from rich.text import Text

from src.dataverse_apis.core.logging.logging_conf import get_logger
from src.dataverse_apis.features.account.account_operations import (
    deactivate_account_with_note,
    reactivate_account_and_delete_note
)
from ..fetch_accounts import (
    get_account_id_by_bus_id,
    get_column_name,
)

logger = get_logger(__name__)

DEFAULT_ACCOUNT_ID_COLUMN = "account_id"
DEFAULT_DATA_DIR = Path(__file__).resolve().parents[2] / "data"
DEFAULT_OUTPUT_PATH = DEFAULT_DATA_DIR / "deactivated_accounts_results.xlsx"
DEFAULT_REACTIVATION_OUTPUT_NAME = "reactivated_accounts_results.xlsx"

def _load_df(df_or_path: Union[pd.DataFrame, str, Path],
             data_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    The primary parameter can be:
    - a DataFrame, or
    - a filename/path (str or Path).
    If it's just a filename, it searches within `data_dir` (default, dataverse_apis/data).
    """
    if isinstance(df_or_path, pd.DataFrame):
        return df_or_path.copy()

    # Is it a string or a Path -> file?
    data_dir = data_dir or DEFAULT_DATA_DIR

    path = Path(df_or_path)
    if not path.is_absolute():
        path = data_dir / path  # assumes that only the name comes "file.xlsx"

    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    if path.suffix.lower() in [".xlsx", ".xls"]:
        return pd.read_excel(path)
    elif path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported file extension: {path.suffix}")
    
def _resolve_account_ids_from_df(
    df: pd.DataFrame,
    bus_id_columns: Optional[Sequence[str]] = None,
    account_id_column: str = DEFAULT_ACCOUNT_ID_COLUMN,
) -> pd.DataFrame:
    df = df.copy()

    if not bus_id_columns:
        bus_id_columns = [get_column_name()]  # "BUS ID" by default

    if not any(col in df.columns for col in bus_id_columns):
        raise ValueError(
            f"None of the BUS ID columns were found in the DataFrame. "
            f"Expected any of: {bus_id_columns}. Columns present: {list(df.columns)}"
        )

    if account_id_column not in df.columns:
        df[account_id_column] = None
        
    pbar = tqdm(
        df.iterrows(),
        total=len(df),
        desc="Resolving account IDs",
        unit="row",
        leave=True
    )
    
    for idx, row in pbar:
        # If you already have an account ID, you don't need to call anything.
        if pd.notna(row.get(account_id_column)):
            # Display BUS ID even if it's already resolved
            bus_id_preview = None
            for col in bus_id_columns:
                if col in df.columns and pd.notna(row[col]):
                    bus_id_preview = str(row[col]).strip()
                    break
            if bus_id_preview:
                pbar.set_postfix({"BUS ID": bus_id_preview})
            continue

        # Detect the BUS ID for this row
        bus_id_value = None
        for col in bus_id_columns:
            if col in df.columns and pd.notna(row[col]):
                bus_id_value = str(row[col]).strip()
                break

        # There is no BUS ID, nothing to do
        if not bus_id_value:
            pbar.set_postfix({"BUS ID": "N/A"})
            continue

        # We show in the bar which one is being processed
        pbar.set_postfix({"BUS ID": bus_id_value})

        # Call the original service to obtain the AccountID
        account_id = get_account_id_by_bus_id(bus_id_value)

        # Save in cell
        df.at[idx, account_id_column] = account_id

    pbar.close()
    return df

def call_deactivate_accounts(
    df: Union[pd.DataFrame, str, Path],
    bus_id_columns: Optional[Sequence[str]] = None,
    reason_column: Optional[str] = None,
    reason_text: Optional[str] = None,
    performed_by: Optional[str] = None,
    output_path: Optional[Path | str] = None,
    data_dir: Optional[Path | str] = None,
) -> pd.DataFrame:
    """
    Main task.

    - `df`: can be a DataFrame or the file name (e.g., "accounts_to_deactivate_ICPS.xlsx").
    - `bus_id_columns`: list of columns where to search for the BUS ID (e.g., ["BUS ID"]).
    - `reason_column`: name of the column that contains the reason for each row (optional).
    - `reason_text`: fixed reason text to use for all rows (optional).
    - `performed_by`: who performs the action (included in the note).
    - `output_path`: path to the results .xlsx file (default: data/deactivated_accounts_results.xlsx).
    - `data_dir`: folder where to find the files (default: dataverse_apis/data).
    """
    if df is None:
        raise ValueError("Input argument `df` is None.")

    # Start time
    start_time = datetime.now()
    logger.info(
        "call_deactivate_accounts started at %s",
        start_time.isoformat(timespec="seconds"),
    )

    # 1) Load DataFrame (if it comes as a file)
    data_dir_path = Path(data_dir) if data_dir else DEFAULT_DATA_DIR
    df_loaded = _load_df(df, data_dir=data_dir_path)

    if df_loaded.empty:
        raise ValueError("Input DataFrame is empty after loading.")

    # 2) Resolve accountids (if needed)
    df_with_ids = _resolve_account_ids_from_df(df_loaded, bus_id_columns=bus_id_columns)
    
    ids_output = None
    if isinstance(df, (str, Path)):
        input_path = Path(df)
        if not input_path.is_absolute():
            input_path = data_dir_path / input_path

        ids_output = input_path.with_name(
            f"{input_path.stem}_with_ids{input_path.suffix}"
        )

        df_with_ids.to_excel(ids_output, index=False)
        logger.info("Saved DataFrame with resolved account IDs to %s", ids_output)

    results: List[Dict[str, Any]] = []
    
    console = Console()

    # 3) Iterate with tqdm
    pbar = tqdm(
        df_with_ids.iterrows(),
        total=len(df_with_ids),
        desc="Deactivating accounts",
        unit="account",
        leave=True
    )
    
    for idx, row in pbar:

        account_id = row.get(DEFAULT_ACCOUNT_ID_COLUMN)

        # No account ID (not found)
        if not account_id or pd.isna(account_id):
            bus_id_preview = None
            for col in bus_id_columns or []:
                if col in df_with_ids.columns and pd.notna(row[col]):
                    bus_id_preview = str(row[col]).strip()
                    break

            pbar.set_postfix({"BUS ID": bus_id_preview or "N/A", "status": "NO ACCOUNT ID"})

            results.append(
                {
                    "bus_id": bus_id_preview,
                    "account_id": None,
                    "deactivated": False,
                    "note_created": False,
                    "error": "accountid not found",
                }
            )
            continue

        # Reason per row (if any)
        row_reason = None
        if reason_column and reason_column in df_with_ids.columns:
            val = row[reason_column]
            row_reason = str(val).strip() if pd.notna(val) else None

        final_reason = reason_text or row_reason

        # Determine BUS ID for reference
        bus_id_value = None
        if bus_id_columns:
            for col in bus_id_columns:
                if col in df_with_ids.columns and pd.notna(row[col]):
                    bus_id_value = str(row[col]).strip()
                    break
        else:
            default_col = get_column_name()
            if default_col in df_with_ids.columns and pd.notna(row[default_col]):
                bus_id_value = str(row[default_col]).strip()

        # Show progress (current BUS ID)
        pbar.set_postfix({"BUS ID": bus_id_value or "N/A"})

        # Call the feature
        resp = deactivate_account_with_note(
            account_id=account_id,
            reason=final_reason,
            performed_by=performed_by,
        )

        error_msg = str(resp.get("error") or "")

        results.append({
            "bus_id": bus_id_value,
            **resp,
        })

        # Special handling of expired token
        if "401" in error_msg:
            pbar.set_postfix({"BUS ID": bus_id_value, "status": "❌ TOKEN EXPIRED"})
            pbar.close()

            # Display important message in red with RICH
            console.print(
                Text(f"[STOPPED] Token expired at BUS ID: {bus_id_value}", style="bold red")
            )
            break

    pbar.close()    
    results_df = pd.DataFrame(results)

    # 4) Save results
    if output_path is None:
        output_path = DEFAULT_OUTPUT_PATH
    else:
        output_path = Path(output_path)
        if not output_path.is_absolute():
            # always save under dataverse_apis/data, using only the filename
            output_path = DEFAULT_DATA_DIR / output_path.name

    output_path.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_excel(output_path, index=False)

    # Time to end
    end_time = datetime.now()
    duration_sec = (end_time - start_time).total_seconds()
    logger.info(
        "call_deactivate_accounts finished at %s (duration: %.2f seconds)",
        end_time.isoformat(timespec="seconds"),
        duration_sec,
    )

    return results_df

def call_reactivate_accounts(
    df: Union[pd.DataFrame, str, Path],
    bus_id_columns: Optional[Sequence[str]] = None,
    note_id_column: Optional[str] = None,
    output_path: Optional[Path | str] = None,
    data_dir: Optional[Path | str] = None,
) -> pd.DataFrame:
    """
    Reactivate accounts and delete the deactivation note.

    - df: DataFrame or filename (e.g., 'deactivated_accounts_results.xlsx' or 'accounts_to_deactivate_ICPS_with_ids.xlsx').
    - bus_id_columns: Columns where to search for the BUS ID (e.g., ['BUS ID']).
    - note_id_column: Name of the column containing the GUID of the note to be deleted (e.g., 'annotationid' if extracted from note_response).
    - output_path: Name of the output file (saved in the same folder as the input).
    """
    if df is None:
        raise ValueError("Input argument `df` is None.")

    data_dir_path = Path(data_dir) if data_dir else DEFAULT_DATA_DIR
    df_loaded = _load_df(df, data_dir=data_dir_path)

    if df_loaded.empty:
        raise ValueError("Input DataFrame is empty after loading.")

    # 1) Resolve accountid (if necessary)
    df_with_ids = _resolve_account_ids_from_df(
        df_loaded,
        bus_id_columns=bus_id_columns,
        account_id_column=DEFAULT_ACCOUNT_ID_COLUMN,
    )

    results: List[Dict[str, Any]] = []
    console = Console()

    pbar = tqdm(
        df_with_ids.iterrows(),
        total=len(df_with_ids),
        desc="Reactivating accounts",
        unit="account",
        leave=True,
    )

    for idx, row in pbar:
        account_id = row.get(DEFAULT_ACCOUNT_ID_COLUMN)

        # Determine BUS ID for reference
        bus_id_value = None
        if bus_id_columns:
            for col in bus_id_columns:
                if col in df_with_ids.columns and pd.notna(row[col]):
                    bus_id_value = str(row[col]).strip()
                    break
        else:
            default_col = get_column_name()
            if default_col in df_with_ids.columns and pd.notna(row[default_col]):
                bus_id_value = str(row[default_col]).strip()

        pbar.set_postfix({"BUS ID": bus_id_value or "N/A"})

        if not account_id or pd.isna(account_id):
            results.append(
                {
                    "bus_id": bus_id_value,
                    "account_id": None,
                    "reactivated": False,
                    "note_deleted": False,
                    "error": "accountid not found",
                }
            )
            continue

        # Get note_id if column exists
        note_id = None
        if note_id_column and note_id_column in df_with_ids.columns:
            raw_val = row[note_id_column]
            if pd.notna(raw_val):
                note_id = str(raw_val).strip()

        resp = reactivate_account_and_delete_note(
            account_id=str(account_id).strip(),
            note_id=note_id,
        )

        results.append({
            "bus_id": bus_id_value,
            **resp,
        })
        
        error_msg = str(resp.get("error") or "")
        
        # Special handling of expired token
        if "401" in error_msg:
            pbar.set_postfix({"BUS ID": bus_id_value, "status": "❌ TOKEN EXPIRED"})
            pbar.close()

            # Display important message in red with RICH
            console.print(
                Text(f"[STOPPED] Token expired at BUS ID: {bus_id_value}", style="bold red")
            )
            break

    pbar.close()

    results_df = pd.DataFrame(results)

    # 2) Determine output folder based on input
    if isinstance(df, (str, Path)):
        input_path = Path(df)
        if not input_path.is_absolute():
            input_path = data_dir_path / input_path
        output_folder = input_path.parent
    else:
        output_folder = data_dir_path

    # Filename
    if output_path is None:
        output_filename = DEFAULT_REACTIVATION_OUTPUT_NAME
    else:
        output_filename = Path(output_path).name

    final_output = output_folder / output_filename
    final_output.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_excel(final_output, index=False)

    return results_df