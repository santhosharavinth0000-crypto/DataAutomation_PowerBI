import os
import pandas as pd
import glob

BASE_PATH = r"H:\IN\MAA\Commercial\MAAGSCDOC\REPORTS\Imports\Kandy\Restore_INC0968185\Kandy\SCID\Longstanding\Reports"
OUTPUT_PATH = r"C:\Users\SMB140\OneDrive - Maersk Group\Desktop\ExtractReportsV2\DRD"
SUMMARY_FILE = os.path.join(OUTPUT_PATH, "SummaryLog.csv")

# Only these headers will be extracted from the sheet
SHEET_HEADERS = ["Days", "Shipment Number", "Last Move", "Eqp Type", "COMMENTS"]
META_HEADERS = ["Year", "Month", "Week", "Date", "Country", "Source"]
EXTRACT_HEADERS = SHEET_HEADERS + META_HEADERS


def extract_from_excel(file_path, sheet_name, year, month, week, date, country):
    try:
        print(f"  Reading {file_path} [{sheet_name}]")
        if file_path.lower().endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.lower().endswith('.xls'):
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        else:
            df = pd.read_excel(file_path, sheet_name=sheet_name, engine="openpyxl")

        # Only check for required columns
        missing = [col for col in SHEET_HEADERS if col not in df.columns]
        if missing:
            print(f"⚠️ Skipping {file_path} ({sheet_name}) → Missing: {missing}")
            return pd.DataFrame()

        df = df[SHEET_HEADERS].copy()

        # Add metadata
        df["Year"] = year
        df["Month"] = month
        df["Week"] = week
        df["Date"] = date
        df["Country"] = country
        df["Source"] = sheet_name

        # Reorder
        df = df[EXTRACT_HEADERS]
        return df

    except Exception as e:
        print(f"❌ Error reading {file_path} ({sheet_name}): {e}")
        return pd.DataFrame()


def main():
    year = input("Enter Year (e.g. 2025): ").strip()
    month = input("Enter Month (e.g. July): ").strip()
    weeks_input = input("Enter specific weeks (comma separated) or press Enter for all: ").strip()

    root_path = os.path.join(BASE_PATH, year, month)
    print(f"🔎 Looking inside: {root_path}")
    if not os.path.exists(root_path):
        print(f"❌ Path not found: {root_path}")
        return

    if weeks_input:
        weeks = [f"WK {w.strip()}" for w in weeks_input.split(",")]
    else:
        weeks = [w for w in os.listdir(root_path) if w.startswith("WK")]

    all_data = []
    summary_records = []

    for week in weeks:
        week_path = os.path.join(root_path, week)
        if not os.path.exists(week_path):
            print(f"⚠️ Week folder not found: {week_path}")
            continue

        print(f"\n📂 Processing week: {week_path}")

        files_found = []
        for ext in ("*.xlsx", "*.xls", "*.xlsm", "*.xlsb", "*.xltx", "*.xltm", "*.csv"):
            files_found.extend(glob.glob(os.path.join(week_path, "**", ext), recursive=True))

        print(f"  Found {len(files_found)} files")

        all_in_folder = []
        for root, _, files in os.walk(week_path):
            for f in files:
                all_in_folder.append(os.path.join(root, f))

        print(f"  Windows shows {len(all_in_folder)} total files (all types)")

        for file_path in files_found:
            country = os.path.splitext(os.path.basename(file_path))[0]
            date = week

            v_df = extract_from_excel(file_path, "VReport", year, month, week, date, country)
            if not v_df.empty:
                all_data.append(v_df)

            x_df = extract_from_excel(file_path, "XReport", year, month, week, date, country)
            if not x_df.empty:
                all_data.append(x_df)

        summary_records.append({
            "Year": year,
            "Month": month,
            "Week": week,
            "TotalFiles_Windows": len(all_in_folder),
            "FilesPickedByPython": len(files_found),
            "ShipmentsExtracted": sum(len(df) for df in all_data if not df.empty)
        })

    # Save ConsolidatedReports as fresh file
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        os.makedirs(OUTPUT_PATH, exist_ok=True)
        output_file = os.path.join(OUTPUT_PATH, "ConsolidatedReports.csv")
        final_df.to_csv(output_file, index=False)

        print(f"\n✅ Fresh Consolidated CSV saved: {output_file} ({len(final_df)} rows)")
        print(final_df.head(10))
    else:
        print("\n⚠️ No data extracted.")

    # Save SummaryLog as fresh file
    if summary_records:
        summary_df = pd.DataFrame(summary_records)
        os.makedirs(OUTPUT_PATH, exist_ok=True)
        summary_df.to_csv(SUMMARY_FILE, index=False)

        print(f"📝 Fresh Summary log saved: {SUMMARY_FILE}")
        print(summary_df.tail(10))


if __name__ == "__main__":
    main()