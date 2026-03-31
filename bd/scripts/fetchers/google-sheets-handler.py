# google_sheets_handler.py

import polars as pl
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from typing import Optional

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",  # needed for Shared Drive access
]


def get_sheets_service(service_account_file: str):
    """Build the Sheets API client from a service account JSON key file."""
    creds = Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def read_sheet(
    service,
    spreadsheet_id: str,
    sheet_name: str = "Sheet1",
    range_: Optional[str] = None,
    header_row: bool = True,
) -> pl.DataFrame:
    """
    Read a Google Sheet into a Polars DataFrame.

    Args:
        service:         Sheets API service object (from get_sheets_service).
        spreadsheet_id:  The spreadsheet ID from the URL.
        sheet_name:      Tab name inside the spreadsheet.
        range_:          Optional A1-notation range, e.g. "A1:D100".
                         Defaults to the entire sheet.
        header_row:      Whether the first row contains column names.

    Returns:
        pl.DataFrame
    """
    sheet_name = f"'{sheet_name}'"
    a1_range = f"{sheet_name}!{range_}" if range_ else sheet_name

    result = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=a1_range,
            # includeGridData not needed for .values().get()
        )
        .execute()
    )

    rows = result.get("values", [])
    if not rows:
        return pl.DataFrame()

    if header_row:
        headers = rows[0]
        data = rows[1:]
        # Pad short rows so every row has the same number of columns
        data = [row + [""] * (len(headers) - len(row)) for row in data]
        return pl.DataFrame(data, schema=headers, orient="row", infer_schema_length=500)
    else:
        return pl.DataFrame(rows, orient="row")


def write_sheet(
    service,
    spreadsheet_id: str,
    df: pl.DataFrame,
    sheet_name: str = "Sheet1",
    start_cell: str = "A1",
    include_header: bool = True,
    value_input_option: str = "USER_ENTERED",  # or "RAW"
) -> dict:
    """
    Write a Polars DataFrame to a Google Sheet.

    Args:
        service:              Sheets API service object.
        spreadsheet_id:       The spreadsheet ID from the URL.
        df:                   Polars DataFrame to write.
        sheet_name:           Tab name inside the spreadsheet.
        start_cell:           Top-left cell to begin writing, e.g. "A1".
        include_header:       Whether to write column names as the first row.
        value_input_option:   "USER_ENTERED" parses formulas/dates;
                              "RAW" writes strings literally.

    Returns:
        API response dict with updatedRange, updatedRows, updatedColumns.
    """
    # Convert all values to strings (Sheets API expects List[List[Any]])
    header = [df.columns] if include_header else []
    body_rows = df.cast(pl.String).rows()  # list of tuples
    all_rows = header + [list(row) for row in body_rows]

    range_notation = f"{sheet_name}!{start_cell}"

    response = (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=range_notation,
            valueInputOption=value_input_option,
            body={"values": all_rows},
        )
        .execute()
    )
    return response


def clear_sheet(service, spreadsheet_id: str, sheet_name: str = "Sheet1") -> dict:
    """Clear all values from a sheet tab without deleting the tab itself."""
    return (
        service.spreadsheets()
        .values()
        .clear(spreadsheetId=spreadsheet_id, range=sheet_name, body={})
        .execute()
    )


def append_to_sheet(
    service,
    spreadsheet_id: str,
    df: pl.DataFrame,
    sheet_name: str = "Sheet1",
    value_input_option: str = "USER_ENTERED",
) -> dict:
    """
    Append rows to the first empty row after existing data.
    Useful for log-style tables — does NOT overwrite existing rows.
    """
    rows = [list(row) for row in df.cast(pl.String).rows()]
    return (
        service.spreadsheets()
        .values()
        .append(
            spreadsheetId=spreadsheet_id,
            range=sheet_name,
            valueInputOption=value_input_option,
            insertDataOption="INSERT_ROWS",
            body={"values": rows},
        )
        .execute()
    )

if __name__ == "__main__":
    
    service = get_sheets_service(r'read-and-write-491623-13e3c9210eef.json')
    df = read_sheet(service, '1AW69JzaCkO_TsSYF7SyWhMYNUuNLB4GS7BFshFWRfNY')
    print(df.schema)
    print(df.head(5))
