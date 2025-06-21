import os
import gspread
from dataentryautomation import read_csv, batch_read_worksheet, find_shop_name_column, find_shop_row, safe_cell_value, month_to_column

# Access google sheets
spreadsheet_key = "1eeDgkSmxboC1FEBCL5EYaBaDcV0FfcalWuwc0mLifFw"
credentials_path = os.path.abspath("dataentryautomation2-4d9294529b45.json")

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

    csv_file_path = os.path.abspath("09 - 05 -25 - Sheet1.csv")
    data_list = read_csv(csv_file_path)

    worksheet_cache = {sheet.title: batch_read_worksheet(sheet) for sheet in worksheet_list}

    batch_updates = {}
    floor_map = {
        "1": "UPPER",
        "2": "ENTRANCE",
        "3": "FIRST",
        "4": "SECOND"
    }

    for shop_name, _, month_year_pairs in data_list:
        if not month_year_pairs:
            print(f"⚠️ No month/year data for {shop_name} - skipping")
            continue

        floor_info = shop_name[1]
        floor_category = floor_map.get(floor_info)
        if not floor_category:
            print(f"⚠️ Unknown floor category for shop {shop_name}")
            continue

        for month, year in month_year_pairs:
            sheet_name = f"{floor_category} {year}"

            if sheet_name not in worksheet_cache:
                print(f"❌ Sheet '{sheet_name}' not found")
                continue

            print(f"\n📄 Sheet name: {sheet_name}")
            sheet_data = worksheet_cache[sheet_name]

            col_idx = find_shop_name_column(sheet_data, sheet_name)
            row_idx = find_shop_row(sheet_data, shop_name, sheet_name)

            if not col_idx or not row_idx:
                print(f"❌ Shop {shop_name} not found in {sheet_name}")
                continue

            # Get the paid column (e.g., AM, AP...)
            paid_col_letter = month_to_column.get(month.lower())
            if not paid_col_letter:
                print(f"⚠️ Invalid month '{month}'")
                continue

            try:
                paid_col_index = gspread.utils.a1_range_to_grid_range(f"{paid_col_letter}1")["startColumnIndex"]
                correct_col_index = paid_col_index - 1  # The actual expected value is one column to the left
                correct_val = safe_cell_value(sheet_data, row_idx - 1, correct_col_index)
                paid_val = safe_cell_value(sheet_data, row_idx - 1, paid_col_index)

                try:
                    expected = int(float(correct_val.replace(",", "").strip()))
                except:
                    print(f"⚠️ Invalid expected value in {sheet_name} {month} at {correct_col_index+1}: {correct_val}")
                    continue

                if str(paid_val).strip() == "":
                    continue  # Skip blank cells

                try:
                    actual = int(float(paid_val.replace(",", "").strip()))
                except:
                    print(f"⚠️ Non-numeric value in {paid_col_letter}{row_idx} of {sheet_name}, fixing to {expected}")
                    actual = None

                if actual != expected:
                    cell_label = f"{paid_col_letter}{row_idx}"
                    batch_updates.setdefault(sheet_name, []).append({
                        'range': cell_label,
                        'values': [[expected]]
                    })
                    print(f"🔧 Correcting {sheet_name} {cell_label}: {actual} ➡️   {expected}")

            except Exception as e:
                print(f"❌ Failed to process {sheet_name} - {month}: {e}")
                continue

    # Perform batch updates
    print("\n📤 Executing batch updates...")
    for sheet_name, updates in batch_updates.items():
        if updates:
            try:
                worksheet = sheet_obj.worksheet(sheet_name)
                worksheet.batch_update(updates)
                print(f"✅ Batch update completed for {sheet_name}.")
            except Exception as e:
                print(f"❌ Failed to update sheet {sheet_name}: {e}")
        else:
            print(f"⚠️ No corrections needed for {sheet_name}.")

if __name__ == "__main__":
    main()
