import gspread
import os
from os import environ, path
import time
import csv
import re
from gspread.exceptions import APIError


# Access google sheets
spreadsheet_key = "1eeDgkSmxboC1FEBCL5EYaBaDcV0FfcalWuwc0mLifFw"
credentials_path = os.path.abspath("datae-424709-b704cb6fe15c.json")


month_to_column = {
    "jan": "AM", "feb": "AP", "mar": "AS", "apr": "AV", "may": "AY",
    "jun": "BB", "jul": "BE", "aug": "BH", "sep": "BK", "oct": "BN",
    "nov": "BQ", "dec": "BT"
}


def exponential_backoff(func):
    def wrapper(*args, **kwargs):
        max_attempts = 5
        attempt = 0
        while attempt < max_attempts:
            try:
                return func(*args, **kwargs)
            except APIError as e:
                if 'Quota exceeded' in str(e):
                    sleep_time = 2 ** attempt
                    print(f"Quota exceeded. Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    attempt += 1
                else:
                    raise e
        raise Exception("Max retries exceeded.")
    return wrapper


@exponential_backoff
def batch_read_worksheet(worksheet):
    """Reads the entire worksheet into a local cache."""
    data = worksheet.get_all_values()
    return data


def find_shop_name_column(worksheet_data, sheet_name):
    """Finds the column index of the "SHOP NO" header in the worksheet data.

    Args:
        worksheet_data: The worksheet data as a list of lists.
        sheet_name: The name of the sheet.

    Returns:
        The column index of the "SHOP NO" header, or None if not found.
    """
    # Get the first row of values
    header_row = worksheet_data[0]

    # Check for Entrance sheet and handle leading space
    sheet_name = sheet_name.upper()  # Get and uppercase sheet name
    print(f"Sheet name: {sheet_name}")

    # Remove leading/trailing spaces from headers for comparison
    header_row = [header.strip().upper() for header in header_row]

    if sheet_name.startswith("ENTRANCE"):
        try:
            return header_row.index(" SHOP NO") + 1  # No leading space
        except ValueError:
            try:
                return header_row.index("SHOP NO") + 1  # Search for leading space
            except ValueError:
                print("Error: 'SHOP NO' header not found in Entrance sheet")
                return None
    else:
        # Search for header in other sheets (no leading space)
        try:
            return header_row.index("SHOP NO") + 1
        except ValueError:
            print("Error: 'SHOP NO' header not found in sheet")
            return None

    
def find_shop_row(worksheet_data, shop_name, sheet_name):
    """Finds the row number where the shop name exists in the worksheet data.

    Args:
        worksheet_data: The worksheet data as a list of lists.
        shop_name: The name of the shop to search for.
        sheet_name: The name of the sheet.

    Returns:
        The row number of the shop if found, otherwise None.
    """
    shop_name_col_index = find_shop_name_column(worksheet_data, sheet_name)
    
    if shop_name_col_index is None:
        print("Error: Could not find 'SHOP NO' column in the worksheet data.")
        return None
    print(f"Searching for {shop_name} in column {shop_name_col_index}")

    for row_num, row_data in enumerate(worksheet_data[1:], start=2):
        shop_no_value = row_data[shop_name_col_index - 1]
        if shop_no_value is not None and shop_name.upper() == shop_no_value.upper():
            return row_num
    return None


def parse_months_year(months_year_str):
    """Parse the months and year string into a list of (month, year) tuples.
    
    Args:
        months_year_str: the value in the Months and Year column

    Returns:
        The single month and year in list format
    """
    months_year_str = months_year_str.strip().upper()
    year_match = re.search(r'\d{2,4}', months_year_str)
    if year_match:
        year_abbr = year_match.group(0)
        year = int(f"20{year_abbr[-2:]}")  # Assuming the year is always in the 2000s
    else:
        year = None

    months = re.split(r'\d{2,4}', months_year_str)[0].strip()

    # Handle month ranges (e.g., JUN-JUL)
    if '-' in months:
        start_month, end_month = months.split('-')
        start_month = start_month.strip()[:3]
        end_month = end_month.strip()[:3]
        start_index = list(month_to_column.keys()).index(start_month.lower())
        end_index = list(month_to_column.keys()).index(end_month.lower())
        return [(month, year) for month in list(month_to_column.keys())[start_index:end_index + 1]]
    
    # Single month
    single_month = months.strip()[:3]
    return [(single_month, year)]


def read_csv(csv_file_path):
    """Read the CSV file and extract relevant data.
    
    Args:
        csv_file_path: The file path of downloaded csv file for inputs of that day

    Returns:
        The data list of inputs
    """
    data_list = []
    try:
        with open(csv_file_path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) < 2 or not row[0].strip():
                    continue
                shop_name = row[0].strip()
                try:
                    amount_paid = int(row[1].strip())
                except ValueError:
                    print(f"Error: Invalid amount_paid for shop {shop_name}")
                    continue
                months_year_str = row[2].strip()
                month_year_pairs = parse_months_year(months_year_str)
                data_list.append((shop_name, amount_paid, month_year_pairs))
    except FileNotFoundError:
        print(f"Error: The file at {csv_file_path} does not exist.")
    return data_list


def handle_backlog_payments(shop_name, amount_paid, years):
    """Distributes the backlog payment over the specified years."""
    month_year_pairs = []
    for year in years:
        for month in month_to_column.keys():
            month_year_pairs.append((month, year))
    return [(shop_name, amount_paid, month_year_pairs)]


def main():
    if not credentials_path:
        print("Error: Could not find credentials_path")
        return

    gc = gspread.service_account(filename=credentials_path)
    sheet_obj = gc.open_by_key(spreadsheet_key)
    print("Successfully connected to Google Sheet")

    csv_file_path = os.path.abspath("new-test - Sheet1.csv")
    data_list = read_csv(csv_file_path)
    for data in data_list:
        print(data)

    batch_updates = {}

    worksheet_data_cache = {}
    worksheet_list = sheet_obj.worksheets()
    for worksheet in worksheet_list:
        worksheet_data_cache[worksheet.title] = batch_read_worksheet(worksheet)

    for data in data_list:
        shop_name, amount_paid, month_year_pairs = data
        for month, year_paid in month_year_pairs:
            floor_info_match = shop_name[1]
            if floor_info_match == "1":
                floor_category = "UPPER"
            elif floor_info_match == "2":
                floor_category = "ENTRANCE"
            elif floor_info_match == "3":
                floor_category = "FIRST"
            elif floor_info_match == "4":
                floor_category = "SECOND"
            else:
                print(f"Error: Invalid shop name format. Unknown floor category for {shop_name}")
                continue
            sheet_name = f"{floor_category} {year_paid}"
            if sheet_name not in worksheet_data_cache:
                print(f"Error: Sheet {sheet_name} not found.")
                continue

            worksheet_data = worksheet_data_cache[sheet_name]
            row_index = find_shop_row(worksheet_data, shop_name, sheet_name)
            if row_index is None:
                print(f"Error: Could not find row for {shop_name} in {sheet_name}")
                continue

            amount_per_month = amount_paid / len(month_year_pairs)
            column_index = month_to_column.get(month.lower())
            if column_index is None:
                print(f"Error: Invalid column index for {shop_name} in {sheet_name}")
                continue

            max_rows = len(worksheet_data)
            max_columns = len(worksheet_data[0])
            column_index_num = gspread.utils.a1_to_rowcol(column_index + '1')[1]
            if row_index > max_rows or column_index_num > max_columns:
                print(f"Error: Cell {column_index}{row_index} exceeds sheet limits.")
                continue

            cell_label = f"{column_index}{row_index}"
            if sheet_name not in batch_updates:
                batch_updates[sheet_name] = []
            batch_updates[sheet_name].append({
                'range': cell_label,
                'values': [[amount_per_month]]
            })
            print(f"Updating cell {cell_label} with value {amount_per_month}")
        time.sleep(1)

    print("Preparing batch updates...")
    for sheet_name, updates in batch_updates.items():
        if updates:
            request_body = {
                'valueInputOption': 'USER_ENTERED',
                'data': [{'range': update['range'], 'values': update['values']} for update in updates]
            }
            print(f"Batch updating sheet {sheet_name} with {len(updates)} updates.")
            sheet_obj.worksheet(sheet_name).batch_update(request_body)
            print(f"Batch update completed for {sheet_name}.")
        else:
            print(f"No valid data to update for {sheet_name}.")

if __name__ == "__main__":
    main()

"""
Features to add:
- Backlog payment will pay from 2015 up to 2024 
- Bring out the errors encountered in code (i.e errors like no year, invalid number of columns, e.t.c)
"""
