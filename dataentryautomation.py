import gspread
import os
from os import environ, path
import time
import csv
import random
import re
from functools import wraps


# Access google sheets
spreadsheet_key = "1eeDgkSmxboC1FEBCL5EYaBaDcV0FfcalWuwc0mLifFw"
credentials_path = os.path.abspath("dataentryautomation2-4d9294529b45.json")


month_to_column = {
    "jan": "AM", "feb": "AP", "mar": "AS", "apr": "AV", "may": "AY",
    "jun": "BB", "jul": "BE", "aug": "BH", "sep": "BK", "oct": "BN",
    "nov": "BQ", "dec": "BT"
}


def exponential_backoff(func=None, *, retries=5, base_delay=3, max_delay=30, post_success_delay=2):
    if func is None:
        return lambda f: exponential_backoff(
            f, retries=retries, base_delay=base_delay, max_delay=max_delay, post_success_delay=post_success_delay
        )

    @wraps(func)
    def wrapper(*args, **kwargs):
        attempt = 0
        delay = base_delay
        while attempt < retries:
            try:
                result = func(*args, **kwargs)
                # Random jitter to avoid collision bursts
                time.sleep(post_success_delay + random.uniform(0.5, 2))
                return result
            except Exception as e:
                print(f"⚠️ {func.__name__} failed on attempt {attempt + 1}: {e}")
                attempt += 1
                if attempt < retries:
                    jitter = random.uniform(0.5, 1.5)
                    time.sleep(min(delay, max_delay) + jitter)
                    delay *= 2  # exponential growth
                else:
                    raise Exception("Max retries exceeded.")
    return wrapper


@exponential_backoff(base_delay=3, post_success_delay=2)
def batch_read_worksheet(sheet):
    return sheet.get_all_values()

@exponential_backoff(base_delay=3, post_success_delay=2)
def batch_update_sheet(sheet, request_body):
    sheet.batch_update(request_body)


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
    # Search for header in other sheets (no leading space)
    try:
        return header_row.index("SHOP NO") + 1
    except ValueError:
        print("Error: 'SHOP NO' header not found in sheet")
        return None

    
def find_shop_row(worksheet_data, shop_name, sheet_name):
    """Finds the row number where the shop name exists in the worksheet data."""
    shop_name_col_index = find_shop_name_column(worksheet_data, sheet_name)
    
    if shop_name_col_index is None:
        print("Error: Could not find 'SHOP NO' column in the worksheet data.")
        return None

    print(f"Searching for {shop_name} in column {shop_name_col_index}")

    shop_name_clean = shop_name.strip().upper()

    for row_num, row_data in enumerate(worksheet_data[1:], start=2):
        if len(row_data) < shop_name_col_index:
            continue  # Skip if row is too short

        shop_no_value = str(row_data[shop_name_col_index - 1]).strip().upper()

        if shop_name_clean == shop_no_value:
            return row_num

    print(f"❌ Shop '{shop_name}' not found in sheet '{sheet_name}'")
    return None


def parse_months_year(months_year_str):
    """Parse month(s) and year from the string like 'JAN - MAR 25' or 'BAL JAN - MAR 25' or 'NOV 24 - APR 25'.
    Returns a list of (month_abbr, year) tuples.
    Raises ValueError if parsing fails or if the string contains 'TOTAL'."""
    
    # Check for TOTAL (case insensitive)
    if 'total' in months_year_str.lower():
        raise ValueError("TOTAL row - skipping")
    
        # Handle BACKLOG explicitly
    if 'backlog' in months_year_str.lower():
        month_names = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                       'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        return [(month, year) for year in range(2015, 2025) for month in month_names]

    # Normalize the string to lowercase
    months_year_str = months_year_str.lower()

    # Remove noise words like 'bal', 'paid', 'sc', 'rent', etc.
    noise_words = ['bal', 'paid', 'sc', 'rent']
    for noise in noise_words:
        months_year_str = months_year_str.replace(noise, '')

    # Remove extra spaces and dashes
    months_year_str = re.sub(r'[-–—]', ' - ', months_year_str)  # normalize all dashes
    months_year_str = re.sub(r'\s+', ' ', months_year_str.strip())  # clean whitespace

    # Extract all year references (2-digit)
    year_matches = re.findall(r'\b(\d{2})\b', months_year_str)
    if not year_matches:
        raise ValueError("Year not found")
    
    # Convert to 4-digit years
    years = [int(y) + (2000 if int(y) < 50 else 1900) for y in year_matches]

    # Extract month tokens
    month_names = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                   'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    tokens = months_year_str.split()
    months = [m for m in tokens if m[:3] in month_names]

    # Handle ranges with '-'
    if '-' in months_year_str:
        try:
            # Get start and end months with their respective years
            start_month = months[0][:3]
            start_idx = month_names.index(start_month)
            
            # Determine if there's a year specified with the start month
            if len(year_matches) > 1:
                start_year = years[0]
                end_year = years[1]
            else:
                start_year = years[0]
                # If end month is before start month in sequence, increment year
                end_month = months[-1][:3]
                end_idx = month_names.index(end_month)
                end_year = start_year + 1 if end_idx < start_idx else start_year
            
            # Generate the range of months
            months_range = []
            current_year = start_year
            current_idx = start_idx
            
            while True:
                months_range.append((month_names[current_idx], current_year))
                
                # Check if we've reached the end month and year
                if (month_names[current_idx] == month_names[month_names.index(months[-1][:3])] and
                    current_year == end_year):
                    break
                
                # Move to next month
                current_idx += 1
                if current_idx >= len(month_names):
                    current_idx = 0
                    current_year += 1
                    
            return months_range
            
        except Exception as e:
            raise ValueError(f"Failed to parse month range: {e}")
    else:
        # Just individual months - use first year found if multiple exist
        return [(m[:3], years[0]) for m in months]
    

def read_csv(csv_file_path):
    """Read the CSV file and extract relevant data."""
    data_list = []
    try:
        with open(csv_file_path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            line_num = 1
            for row in reader:
                print(f"🔍 Reading line {line_num}: {row}")
                line_num += 1

                if len(row) < 3:
                    print(f"⚠️ Skipping line {line_num}: Not enough columns")
                    continue

                shop_name = row[0].strip()
                if not shop_name:
                    print(f"⚠️ Skipping line {line_num}: Empty shop name")
                    continue

                try:
                    amount_paid = int(row[1].strip())
                except ValueError:
                    print(f"❌ Invalid amount '{row[1]}' for shop {shop_name}")
                    continue

                months_year_str = row[2].strip()
                try:
                    month_year_pairs = parse_months_year(months_year_str)
                except Exception as e:
                    print(f"❌ Failed to parse month/year '{months_year_str}': {e}")
                    continue

                print(f"✅ Parsed: {shop_name}, ₦{amount_paid}, {month_year_pairs}")
                data_list.append((shop_name, amount_paid, month_year_pairs))
    except FileNotFoundError:
        print(f"❌ File not found: {csv_file_path}")
    return data_list


def safe_cell_value(data, row, col):
    if 0 <= row < len(data) and 0 <= col < len(data[row]):
        return data[row][col]
    return None

'''
def main():
            
    if not credentials_path:
        print("❌ Error: Could not find credentials_path")
        return

    try:
        gc = gspread.service_account(filename=credentials_path)
        print("✅ Successfully authenticated with Google Sheets API.")
    except Exception as e:
        print(f"❌ Failed to authenticate: {e}")
        return

    try:
        sheet_obj = gc.open_by_key(spreadsheet_key)
        print(f"✅ Successfully opened the spreadsheet: {sheet_obj.title}")
    except Exception as e:
        print(f"❌ Failed to open spreadsheet: {e}")
        return

    try:
        worksheet_list = sheet_obj.worksheets()
    
        print("📄 Found the following worksheets:")
        for sheet in worksheet_list:
            print(f" - {sheet.title}
    except Exception as e:
        print(f"❌ Failed to read worksheets: {e}")
        
    csv_file_path = os.path.abspath("08-03-25 - Sheet1.csv")
    data_list = read_csv(csv_file_path)
        
    # TEST 2: Focused test for shop finding (Point 3)
    print("\n🔍 Testing Shop Finding Logic:")
    
    # Pre-load worksheet data for efficiency
    worksheet_cache = {}
    for sheet in sheet_obj.worksheets():
        worksheet_cache[sheet.title] = batch_read_worksheet(sheet)

    print("\n🔍 Testing Shop Positions:")
    print("=" * 50)
    
    for shop_name, amount_paid, month_year_pairs in data_list:
        # Determine sheet name from shop name (using your existing logic)
        floor_info = shop_name[1]  # Example: "A101" -> "1"
        floor_map = {
            "1": "UPPER",
            "2": "ENTRANCE",
            "3": "FIRST",
            "4": "SECOND"
        }
        floor_category = floor_map.get(floor_info, "UNKNOWN")
        
        # Get most recent year from month_year_pairs
        if not month_year_pairs:
            print(f"⚠️ No month/year data for {shop_name} - skipping")
            continue
            
        year = month_year_pairs[0][1]  # Get year from first month-year pair
        sheet_name = f"{floor_category} {year}"
        
        print(f"\n🛍️ Shop: {shop_name} (Searching in '{sheet_name}')")
        
        # Find position in sheet
        if sheet_name not in worksheet_cache:
            print(f"❌ Sheet '{sheet_name}' not found")
            continue
            
        sheet_data = worksheet_cache[sheet_name]
        col_idx = find_shop_name_column(sheet_data, sheet_name)
        row_idx = find_shop_row(sheet_data, shop_name, sheet_name)
        
        if col_idx and row_idx:
            print(f"✅ Found at: Row {row_idx}, Column {col_idx} ({gspread.utils.rowcol_to_a1(row_idx, col_idx)})")
            print(f"   Header: '{sheet_data[0][col_idx-1]}'")
            print(f"   Cell Value: '{sheet_data[row_idx-1][col_idx-1]}'")
        else:
            print("❌ Position not found")
    
 # Cache all worksheet data for efficient access

    for sheet in worksheet_list:
        worksheet_cache[sheet.title] = batch_read_worksheet(sheet)

    # For each shop, find the row and column, then prepare the batch update
    batch_updates = {}  # This will hold all batch update requests
    for shop_name, amount_paid, month_year_pairs in data_list:
        floor_info = shop_name[1]  # Example: "A101" -> "1"
        floor_map = {
            "1": "UPPER",
            "2": "ENTRANCE",
            "3": "FIRST",
            "4": "SECOND"
        }
        floor_category = floor_map.get(floor_info, "UNKNOWN")
        
        # Get most recent year from month_year_pairs
        if not month_year_pairs:
            print(f"⚠️ No month/year data for {shop_name} - skipping")
            continue
            
        year = month_year_pairs[0][1]  # Get year from first month-year pair
        sheet_name = f"{floor_category} {year}"
        
        print(f"\n🛍️ Shop: {shop_name} (Searching in '{sheet_name}')")
        
        if sheet_name not in worksheet_cache:
            print(f"❌ Sheet '{sheet_name}' not found")
            continue
            
        sheet_data = worksheet_cache[sheet_name]
        col_idx = find_shop_name_column(sheet_data, sheet_name)
        row_idx = find_shop_row(sheet_data, shop_name, sheet_name)
        
        if col_idx and row_idx:
            # Find column index for the month in question (using your month_to_column mapping)
            for month, year_paid in month_year_pairs:
                column_index = month_to_column.get(month.lower())
                if column_index is None:
                    print(f"❌ Invalid month '{month}' for {shop_name} in {sheet_name}")
                    continue
                
                # Special case: BACKLOG
                is_backlog = month_year_pairs[0][0].lower() == "backlog"
                if is_backlog:
                    print(f"🔁 Handling BACKLOG for {shop_name}")

                    total_inserted = 0
                    for year in range(2015, 2025):  
                        sheet_name = f"{floor_category} {year}"
                        if sheet_name not in worksheet_cache:
                            print(f"❌ Sheet '{sheet_name}' not found for backlog year {year}")
                            continue

                        sheet_data = worksheet_cache[sheet_name]
                        col_idx = find_shop_name_column(sheet_data, sheet_name)
                        row_idx = find_shop_row(sheet_data, shop_name, sheet_name)

                        if not col_idx or not row_idx:
                            print(f"❌ Could not find position for {shop_name} in {sheet_name}")
                            continue

                        # Get amount from header
                        header_value = sheet_data[0][col_idx - 1]
                        try:
                            amount_in_header = float(header_value.split('=')[-1].strip())
                        except:
                            print(f"⚠️ Invalid header format for {sheet_name}: {header_value}")
                            continue

                        # Loop through all months in order until one empty cell is found
                        for month, col_letter in month_to_column.items():
                            column_index = gspread.utils.a1_range_to_grid_range(f"{col_letter}1")["startColumnIndex"]
                            try:
                                existing_val = sheet_data[row_idx - 1][column_index]
                            except IndexError:
                                continue

                            if str(existing_val).strip() == "":
                                # Fill this cell
                                cell_label = f"{col_letter}{row_idx}"
                                if sheet_name not in batch_updates:
                                    batch_updates[sheet_name] = []

                                batch_updates[sheet_name].append({
                                    'range': cell_label,
                                    'values': [[int(round(amount_in_header))]]
                                })
                                total_inserted += amount_in_header
                                print(f"✅ Filled {cell_label} with {amount_in_header} (Inserted: {total_inserted} / {amount_paid})")

                                break  # Only one payment per year

                        if total_inserted >= amount_paid:
                            print(f"✅ BACKLOG cleared for {shop_name} with {total_inserted}")
                            break

                    if total_inserted < amount_paid:
                        print(f"⚠️ Not enough unpaid months found for {shop_name}. Inserted only {total_inserted} out of {amount_paid}")

                    continue 
                
                # Calculate the payment amount for the month
                amount_per_month = int(round(amount_paid / len(month_year_pairs)))
                
                # Find the cell where we need to update the value
                cell_label = f"{column_index}{row_idx}"  # Example: "AM12"
                
                # Check if the cell is already filled
                try:
                    existing_value = sheet_data[row_idx - 1][gspread.utils.a1_range_to_grid_range(cell_label)['startColumnIndex']]
                    if str(existing_value).strip() != '':
                        print(f"🔁 Cell {cell_label} already filled. Skipping.")
                        continue
                except IndexError:
                    print(f"⚠️ Could not check value at {cell_label} (index out of bounds)")
                
                if sheet_name not in batch_updates:
                    batch_updates[sheet_name] = []
                    
                batch_updates[sheet_name].append({
                    'range': cell_label,
                    'values': [amount_per_month]
                })
                print(f"Preparing to update cell {cell_label} with value {amount_per_month}")
        
        else:
            print("❌ Shop not found")

        # Execute the batch update for each sheet
        print("📤 Executing batch updates...")
        for sheet_name, updates in batch_updates.items():
            if updates:
                request_body = {
                    'data': [{'range': update['range'], 'values': update['values']} for update in updates],
                    'value_input_option': 'USER_ENTERED',
                }
                print(f"Batch updating sheet {sheet_name} with {len(updates)} updates.")
                try:
                    worksheet = sheet_obj.worksheet(sheet_name)
                    worksheet.batch_update(request_body)
                    print(f"✅ Batch update completed for {sheet_name}.")
                except Exception as e:
                    print(f"❌ Failed to update sheet {sheet_name}: {e}")
            else:
                print(f"⚠️ No valid data to update for {sheet_name}.")
                
            '''     
            
def main():
    if not credentials_path:
        print("❌ Error: Could not find credentials_path")
        return

    try:
        gc = gspread.service_account(filename=credentials_path)
        print("✅ Successfully authenticated with Google Sheets API.")
    except Exception as e:
        print(f"❌ Failed to authenticate: {e}")
        return

    try:
        sheet_obj = gc.open_by_key(spreadsheet_key)
        print(f"✅ Successfully opened the spreadsheet: {sheet_obj.title}")
    except Exception as e:
        print(f"❌ Failed to open spreadsheet: {e}")
        return

    try:
        worksheet_list = sheet_obj.worksheets()
    except Exception as e:
        print(f"❌ Failed to read worksheets: {e}")
        return

    csv_file_path = os.path.abspath("08-03-25 - Sheet1.csv")
    data_list = read_csv(csv_file_path)

    worksheet_cache = {sheet.title: batch_read_worksheet(sheet) for sheet in worksheet_list}

    batch_updates = {}
    floor_map = {
        "1": "UPPER",
        "2": "ENTRANCE",
        "3": "FIRST",
        "4": "SECOND"
    }

    for shop_name, amount_paid, month_year_pairs in data_list:
        if not month_year_pairs:
            print(f"⚠️ No month/year data for {shop_name} - skipping")
            continue

        floor_info = shop_name[1]
        floor_category = floor_map.get(floor_info, "UNKNOWN")
        year = month_year_pairs[0][1]
        sheet_name = f"{floor_category} {year}"

        print(f"\n🛍️ Shop: {shop_name} (Searching in '{sheet_name}')")

        if sheet_name not in worksheet_cache:
            print(f"❌ Sheet '{sheet_name}' not found")
            continue

        sheet_data = worksheet_cache[sheet_name]
        col_idx = find_shop_name_column(sheet_data, sheet_name)
        row_idx = find_shop_row(sheet_data, shop_name, sheet_name)

        if not col_idx or not row_idx:
            print("❌ Shop not found")
            continue

        is_backlog = month_year_pairs[0][0].lower() == "backlog"

        if is_backlog:
            print(f"🔁 Handling BACKLOG for {shop_name}")
            total_inserted = 0

            for backlog_year in range(2015, 2025):
                backlog_sheet = f"{floor_category} {backlog_year}"
                if backlog_sheet not in worksheet_cache:
                    print(f"❌ Sheet '{backlog_sheet}' not found for backlog year {backlog_year}")
                    continue

                data = worksheet_cache[backlog_sheet]
                col = find_shop_name_column(data, backlog_sheet)
                row = find_shop_row(data, shop_name, backlog_sheet)

                if not col or not row:
                    print(f"❌ Could not find position for {shop_name} in {backlog_sheet}")
                    continue

                header_val = safe_cell_value(data, 0, col - 1)
                try:
                    amount_in_header = float(header_val.split('=')[-1].strip())
                except:
                    print(f"⚠️ Invalid header format for {backlog_sheet}: {header_val}")
                    continue

                for month, col_letter in month_to_column.items():
                    col_index = gspread.utils.a1_range_to_grid_range(f"{col_letter}1")["startColumnIndex"]
                    existing = safe_cell_value(data, row - 1, col_index)

                    if str(existing).strip() == "":
                        cell_label = f"{col_letter}{row}"
                        batch_updates.setdefault(backlog_sheet, []).append({
                            'range': cell_label,
                            'values': [[int(round(amount_in_header))]]
                        })
                        total_inserted += amount_in_header
                        print(f"✅ Filled {cell_label} with {amount_in_header} (Inserted: {total_inserted})")
                        break  # One payment per year

                if total_inserted >= amount_paid:
                    print(f"✅ BACKLOG cleared for {shop_name} with {total_inserted}")
                    break

            if total_inserted < amount_paid:
                print(f"⚠️ Not enough unpaid months found for {shop_name}. Inserted {total_inserted} out of {amount_paid}")
            continue  # Done with backlog

        # Normal case
        amount_per_month = int(round(amount_paid / len(month_year_pairs)))

        for month, year_paid in month_year_pairs:
            col_letter = month_to_column.get(month.lower())
            if not col_letter:
                print(f"❌ Invalid month '{month}' for {shop_name}")
                continue

            cell_label = f"{col_letter}{row_idx}"
            col_index = gspread.utils.a1_range_to_grid_range(cell_label)['startColumnIndex']
            existing_val = safe_cell_value(sheet_data, row_idx - 1, col_index)

            if existing_val is not None and str(existing_val).strip() != "":
                print(f"🔁 Cell {cell_label} already filled. Skipping.")
                continue

            batch_updates.setdefault(sheet_name, []).append({
                'range': cell_label,
                'values': [[amount_per_month]]
            })
            print(f"Preparing to update cell {cell_label} with value {amount_per_month}")

    # ✅ Perform batch updates
    print("📤 Executing batch updates...")
    for sheet_name, updates in batch_updates.items():
        if not updates:
            print(f"⚠️ No updates for {sheet_name}")
            continue

        request_body = {
            "data": updates,
            "valueInputOption": "USER_ENTERED"
        }

        print(f"Batch updating sheet {sheet_name} with {len(updates)} updates.")
        try:
            sheet_obj.batch_update(request_body)
            print(f"✅ Batch update completed for {sheet_name}.")
        except Exception as e:
            print(f"❌ Failed to update sheet {sheet_name}: {e}")
   
if __name__ == "__main__":
    main()

