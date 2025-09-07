import pandas as pd
import os
from pathlib import Path
import re
from datetime import datetime
import glob

class ExportLongstandingConsolidator:
    def __init__(self, base_path):
        """
        Initialize the consolidator for Export Longstanding data
        """
        self.base_path = Path(base_path)
        
        # Define output headers as requested
        self.output_headers = [
            "CURRENT_YEARWEEK",
            "ACTLOC_COUNTRY", 
            "CONT_TYPE",
            "DAYS",
            "MOVE",
            "SHIPMENT_NUMBER",
            "Week",
            "Source",
            "Days since Gated Out",
            "ACTLOC Country",
            "Container Type", 
            "Process",
            "Sourcetype",
            "Email Type"
        ]
        
        # Files to ignore
        self.ignore_files = ["Raw Data.xlsx", "DCC Not Sent.xlsx"]

    def clean_days_value(self, days_value):
        """
        Clean and convert DAYS values to numeric format
        Handles cases like: "29 to 35 days", "30", "25 days", "20-25", etc.
        """
        try:
            if pd.isna(days_value) or days_value == '':
                return None
                
            days_str = str(days_value).strip().lower()
            
            # Handle range patterns like "29 to 35 days", "20-25", "15–20"
            range_patterns = [
                r'(\d+)\s*to\s*(\d+)',
                r'(\d+)\s*-\s*(\d+)', 
                r'(\d+)\s*–\s*(\d+)',
                r'(\d+)\s*~\s*(\d+)'
            ]
            
            for pattern in range_patterns:
                match = re.search(pattern, days_str)
                if match:
                    start = int(match.group(1))
                    end = int(match.group(2))
                    return (start + end) / 2  # Return average
            
            # Handle single number patterns like "30 days", "25"
            single_pattern = r'(\d+)'
            match = re.search(single_pattern, days_str)
            if match:
                return int(match.group(1))
            
            return None
        except Exception as e:
            print(f"⚠️ Error cleaning days value '{days_value}': {e}")
            return None

    def is_excel_file(self, filename):
        """Check if file is an Excel file"""
        excel_extensions = ('.xlsx', '.xls', '.xlsm', '.xlsb', '.xltx', '.xltm', '.csv')
        return filename.lower().endswith(excel_extensions)

    def get_file_type(self, filename):
        """Determine file type based on filename"""
        filename_lower = filename.lower()
        
        if "ls template" in filename_lower or "ls templet" in filename_lower:
            return "LS_Template"
        elif "export_empties_longstandings" in filename_lower or "empties longstanding" in filename_lower:
            return "Export_Empties"
        elif "export_longstandings" in filename_lower or "export longstanding" in filename_lower:
            return "Export_Longstandings"
        else:
            return None

    def map_columns_to_output(self, df, file_type):
        """Map columns from different file types to standardized output format"""
        
        # Normalize column names
        df.columns = [str(c).strip() for c in df.columns]
        
        # Create a copy to avoid modifying original
        mapped_df = df.copy()
        
        # Map columns based on file type
        if file_type == "LS_Template":
            # LS Template mappings
            column_mappings = {
                'Booking number': 'SHIPMENT_NUMBER',
                'Days since Gated Out': 'DAYS',
                'ACTLOC Country': 'ACTLOC_COUNTRY',
                'Container Type': 'CONT_TYPE',
                'Process': 'MOVE'
            }
            
        elif file_type in ["Export_Empties", "Export_Longstandings"]:
            # Export files mappings
            column_mappings = {
                'Last move': 'MOVE',
                'ACTLOC_COUNTRY': 'ACTLOC_COUNTRY',
                'CONT_TYPE': 'CONT_TYPE', 
                'DAYS': 'DAYS',
                'SHIPMENT_NUMBER': 'SHIPMENT_NUMBER',
                'CURRENT_YEARWEEK': 'CURRENT_YEARWEEK'
            }
        else:
            column_mappings = {}
        
        # Apply column mappings
        for old_col, new_col in column_mappings.items():
            if old_col in mapped_df.columns:
                mapped_df[new_col] = mapped_df[old_col]
        
        return mapped_df

    def process_file(self, file_path, week_num, file_type):
        """Process a single file"""
        try:
            filename = os.path.basename(file_path)
            file_lower = file_path.lower()
            
            # Read file based on extension
            if file_lower.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_lower.endswith('.xls'):
                df = pd.read_excel(file_path)
            else:
                df = pd.read_excel(file_path, engine="openpyxl")
            
            if df.empty:
                print(f"⚠️ Empty file: {filename}")
                return None
            
            # Map columns to standard format
            df = self.map_columns_to_output(df, file_type)
            
            # Clean DAYS column to numeric (with progress for large files)
            if 'DAYS' in df.columns:
                if len(df) > 10000:
                    print(f"   🔄 Cleaning DAYS values for large file ({len(df)} rows)...")
                df['DAYS'] = df['DAYS'].apply(self.clean_days_value)
            
            # Add metadata columns
            df["Week"] = week_num
            df["Source"] = filename
            df["Sourcetype"] = file_type
            
            # Ensure all required columns exist
            for col in self.output_headers:
                if col not in df.columns:
                    df[col] = ""
            
            # Keep only required columns in correct order
            df = df[self.output_headers]
            
            return df
            
        except Exception as e:
            print(f"⚠️ Error reading {file_path}: {e}")
            return None

    def process_week(self, year, week_num):
        """Process a single week folder"""
        
        # Try different week folder naming conventions
        possible_week_names = [
            f"Week {week_num}",
            f"Week{week_num}",
            f"Wk {week_num}",
            f"Wk{week_num}",
            f"W{week_num}",
            f"{week_num}"
        ]
        
        week_path = None
        for week_name in possible_week_names:
            test_path = self.base_path / str(year) / week_name
            if test_path.exists():
                week_path = test_path
                break
        
        if not week_path or not week_path.exists():
            print(f"❌ Week {week_num} folder not found in {self.base_path / str(year)}")
            return []
        
        print(f"\n🔹 Processing Week {week_num} folder: {week_path}")
        
        all_data = []
        processed_files = 0
        
        # Walk through all subdirectories
        for root, dirs, files in os.walk(week_path):
            for file in files:
                if file in self.ignore_files or not self.is_excel_file(file):
                    continue
                
                # Determine file type
                file_type = self.get_file_type(file)
                if not file_type:
                    continue
                
                file_path = os.path.join(root, file)
                df = self.process_file(file_path, week_num, file_type)
                
                if df is not None and not df.empty:
                    all_data.append(df)
                    processed_files += 1
                    print(f"✅ {file_type}: {file} ({len(df)} rows)")
        
        print(f"📊 Week {week_num} Summary: {processed_files} files processed, {sum(len(df) for df in all_data)} total rows")
        return all_data

    def parse_week_input(self, week_input):
        """Parse week input string to extract list of weeks"""
        week_numbers = []
        
        # Split by comma and process each part
        parts = [part.strip() for part in week_input.split(',')]
        
        for part in parts:
            if '-' in part:
                # Handle range like "26-28"
                try:
                    start, end = part.split('-')
                    start_week = int(start.strip())
                    end_week = int(end.strip())
                    week_numbers.extend(range(start_week, end_week + 1))
                except ValueError:
                    print(f"⚠️ Invalid range format: {part}")
                    continue
            else:
                # Handle single week
                try:
                    week_numbers.append(int(part))
                except ValueError:
                    print(f"⚠️ Invalid week number: {part}")
                    continue
        
        # Remove duplicates and sort
        week_numbers = sorted(list(set(week_numbers)))
        return week_numbers

    def create_master_file(self, df_list, output_dir, year, processed_weeks):
        """Create or update the single master file for Power BI"""
        try:
            if not df_list:
                print("❌ No data to process")
                return None
            
            # Combine all processed weeks data
            current_data_df = pd.concat(df_list, ignore_index=True)
            print(f"📋 Current extraction data: {len(current_data_df)} rows")
            
            # Define master file path
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            master_file_path = output_path / "ExportLongstanding_MasterData.csv"
            
            # Check if master file already exists
            if master_file_path.exists():
                print("📂 Loading existing master file...")
                existing_df = pd.read_csv(master_file_path)
                print(f"📊 Existing data: {len(existing_df)} rows")
                
                # Remove existing data for processed weeks to avoid duplicates
                processed_weeks_str = [str(w) for w in processed_weeks]
                existing_df = existing_df[~existing_df['Week'].astype(str).isin(processed_weeks_str)]
                print(f"📊 After removing weeks {processed_weeks}: {len(existing_df)} rows")
                
                # Combine with new data
                master_df = pd.concat([existing_df, current_data_df], ignore_index=True)
            else:
                print("🆕 Creating new master file...")
                master_df = current_data_df
            
            # Sort by Week and other key columns
            if not master_df.empty:
                master_df = master_df.sort_values(['Week', 'ACTLOC_COUNTRY', 'SHIPMENT_NUMBER'], 
                                                na_position='last').reset_index(drop=True)
            
            # Save master file
            master_df.to_csv(master_file_path, index=False, encoding="utf-8-sig")
            
            print(f"\n✅ Master Export file saved: {master_file_path}")
            print(f"   📊 Total rows in master file: {len(master_df)}")
            print(f"   📅 Processed weeks {processed_weeks} data: {len(current_data_df)} rows")
            
            # Show week breakdown
            if not master_df.empty and 'Week' in master_df.columns:
                week_counts = master_df['Week'].value_counts().sort_index()
                print(f"   📈 Week breakdown:")
                for week, count in week_counts.items():
                    status = "✨ NEW" if int(week) in processed_weeks else ""
                    print(f"     - Week {week}: {count} rows {status}")
            
            # Show file type breakdown
            if not master_df.empty and 'Sourcetype' in master_df.columns:
                type_counts = master_df['Sourcetype'].value_counts()
                print(f"   📂 File type breakdown:")
                for file_type, count in type_counts.items():
                    print(f"     - {file_type}: {count} rows")
            
            # Show DAYS statistics if available
            if 'DAYS' in master_df.columns:
                numeric_days = pd.to_numeric(master_df['DAYS'], errors='coerce').dropna()
                if not numeric_days.empty:
                    print(f"   📈 DAYS Statistics:")
                    print(f"     - Valid numeric values: {len(numeric_days)}/{len(master_df)}")
                    print(f"     - Range: {numeric_days.min():.0f} to {numeric_days.max():.0f} days")
                    print(f"     - Average: {numeric_days.mean():.1f} days")
            
            return master_file_path
            
        except Exception as e:
            print(f"❌ Error creating master file: {e}")
            return None

def main():
    """Interactive mode for processing single or multiple weeks"""
    
    # User input
    year = input("Enter Year (e.g. 2025): ").strip()
    week_input = input("Enter Week number(s) (e.g. 26 or 26,27,28 or 26-28): ").strip()
    
    # Convert year to int
    try:
        year = int(year)
    except ValueError:
        print("❌ Please enter valid numeric year")
        return
    
    # Paths
    BASE_PATH = r"H:\IN\MAA\Commercial\MAAGSCDOC\REPORTS\Imports\Kandy\Restore_INC0968185\Kandy\SCID\Longstanding\Export Longstanding"
    OUTPUT_DIR = r"C:\Users\SMB140\OneDrive - Maersk Group\Desktop\ExtractReportsV2\DRD"
    
    # Initialize consolidator
    consolidator = ExportLongstandingConsolidator(BASE_PATH)
    
    # Parse week input
    week_numbers = consolidator.parse_week_input(week_input)
    
    if not week_numbers:
        print("❌ No valid week numbers found")
        return
    
    print(f"\n🎯 Processing weeks: {week_numbers}")
    
    # Process all weeks
    all_df_list = []
    successful_weeks = []
    failed_weeks = []
    
    for week_num in week_numbers:
        print(f"\n{'='*50}")
        print(f"🔄 PROCESSING WEEK {week_num}")
        print(f"{'='*50}")
        
        df_list = consolidator.process_week(year, week_num)
        
        if df_list:
            all_df_list.extend(df_list)
            successful_weeks.append(week_num)
            print(f"✅ Week {week_num} processed successfully")
        else:
            failed_weeks.append(week_num)
            print(f"❌ Week {week_num} processing failed")
    
    # Summary
    print(f"\n{'='*50}")
    print(f"📋 PROCESSING SUMMARY")
    print(f"{'='*50}")
    print(f"✅ Successfully processed weeks: {successful_weeks}")
    if failed_weeks:
        print(f"❌ Failed weeks: {failed_weeks}")
    
    if not all_df_list:
        print(f"\n⚠️ No valid files found for any of the specified weeks")
        return
    
    # Create master file
    master_file = consolidator.create_master_file(all_df_list, OUTPUT_DIR, year, successful_weeks)
    
    if master_file:
        print(f"\n🎉 Process completed successfully!")
        print(f"💡 Connect your Power BI to: {master_file}")
        print(f"📊 Total weeks processed: {len(successful_weeks)}")
        print(f"📈 Total rows added: {sum(len(df) for df in all_df_list)}")
    else:
        print(f"\n❌ Process failed")

if __name__ == "__main__":
    main()