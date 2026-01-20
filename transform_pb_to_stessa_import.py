import csv
import os

INPUT_CSV = "pb_merged-2025.csv"
OUTPUT_CSV = "pb_stessa_import.csv"

# Mapping based on analysis of buildingNames in PB vs Stessa Properties
PROPERTY_MAP = {
    "1140 West 62nd": "1140 West 62nd",
    "4381 West 22nd Plaza": "4318 West 22nd Plaza",
    "4604 Miller Lane": "4604 Miller Lane",
    "554 Kentucky Street": "554 Kentucky St",
    "839 King Street": "839 King Street"
}

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"Error: {INPUT_CSV} not found.")
        return

    failed_properties = set()
    rows = []

    with open(INPUT_CSV, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            building_name = row.get("buildingName", "")
            stessa_property = PROPERTY_MAP.get(building_name)
            
            if not stessa_property:
                if building_name:
                    failed_properties.add(building_name)
                stessa_property = building_name # Fallback

            # Description: memo | gl_account | ((PBI))
            memo = row.get("postingMemo", "").strip()
            gl_account = row.get("combinedGLAccountName", "").strip()
            description_parts = [p for p in [memo, gl_account] if p]
            description_parts.append("((PBI))")
            description = " | ".join(description_parts)

            # Payee: strip 'Unit 1 - ' prefix
            payee = row.get("payeeName", "").replace("Unit 1 - ", "")

            # Amount: invert the sign
            try:
                raw_amount = float(row.get("amount", "0"))
                inverted_amount = f"{raw_amount * -1:.2f}"
            except ValueError:
                inverted_amount = row.get("amount", "")

            rows.append({
                "Date": row.get("entryDate", ""),
                "Amount": inverted_amount,
                "Payee": payee,
                "Description": description,
                "Category": row.get("Stessa Mapped Sub-Category", ""),
                "Property": stessa_property,
                "Unit": ""
            })

    # Sort records by date ascending
    from datetime import datetime
    def get_date(row):
        try:
            return datetime.strptime(row["Date"], "%m/%d/%Y")
        except ValueError:
            try:
                # Handle cases like "1/1/25" if they exist, though PB usually uses YYYY
                return datetime.strptime(row["Date"], "%m/%d/%y")
            except ValueError:
                return datetime.min # Fallback for headers or invalid dates

    rows.sort(key=get_date)

    output_headers = ["Date", "Amount", "Payee", "Description", "Category", "Property", "Unit"]
    
    with open(OUTPUT_CSV, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=output_headers)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Successfully generated {len(rows)} records in {OUTPUT_CSV}")
    
    if failed_properties:
        print("\nWARNING: The following building names could not be matched to Stessa properties:")
        for fp in sorted(list(failed_properties)):
            print(f" - {fp}")
    else:
        print("\nAll property matches were successful.")

if __name__ == "__main__":
    main()
