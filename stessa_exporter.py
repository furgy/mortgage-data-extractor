import os
import json
import csv
import sys

# Mapping from extracted address (as found in JSON) to Stessa Property Name
PROPERTY_MAPPING = {
    "1700 W FLAMINGO DR CHANDLER AZ 85286": "1700 W Flamingo Dr",
    "3274 EHAWK PL CHANDLER AZ 85286": "3274 E Hawk Pl",
    "4604 MILLER LN GARY IN 46403": "4604 Miller Lane",
    "440 MARIONOAKS LN OCALA FL 34473": "440 Marion Oaks Ln",
    "14977 SW 38 TH CIR OCALA FL 34473": "14977 SW 38th Cir",
    "1140 W 62 ND AVE MERRILLVILLE IN 46410": "1140 West 62nd"
}

def get_stessa_property(extracted_addr):
    # Try exact match
    if extracted_addr in PROPERTY_MAPPING:
        return PROPERTY_MAPPING[extracted_addr]
    
    # Try case-insensitive and partial cleanup for matches
    clean_addr = extracted_addr.replace("_", " ").strip().upper()
    for raw, stessa_name in PROPERTY_MAPPING.items():
        if raw.upper() == clean_addr:
            return stessa_name
    
    return extracted_addr # Fallback to extracted if no mapping

def generate_stessa_csv(input_dir="processed_statements", output_file="stessa_import.csv"):
    rows = []
    
    files = [f for f in os.listdir(input_dir) if f.endswith(".json") and f != "downloads.json"]
    
    for filename in files:
        with open(os.path.join(input_dir, filename), "r") as f:
            data = json.load(f)
        
        bank = data.get("bank", "Unknown")
        loan_num = data.get("loan_number", "0000")
        last_4 = loan_num[-4:] if loan_num else "0000"
        
        # Payee & Description Setup
        if bank == "PNC":
            payee = "PNC Mortgage Payment"
            desc = f"PNC MORTGAGE     PNC PYMT   ***********{last_4}"
        elif bank == "Huntington":
            payee = "Huntington Bank"
            desc = f"HUNTINGTON NAT'L MTG PMTS   ***********{last_4}"
        else:
            payee = bank
            desc = f"Mortgage Payment ***********{last_4}"

        stessa_property = get_stessa_property(data.get("property_address", ""))
        statement_date = data.get("statement_date", "")

        # Transaction Splitting (Principal, Interest, Escrow)
        transactions = [
            ("principal_breakdown", "Mortgage Principal"),
            ("interest_breakdown", "Mortgage Interest"),
            ("escrow_breakdown", "General Escrow Payments")
        ]

        for key, category in transactions:
            amount = data.get(key, "0.00")
            if amount and float(amount.replace(",", "")) > 0:
                rows.append({
                    "Date": statement_date,
                    "Amount": f"-{amount}", # Payments are usually negative in Stessa imports
                    "Payee": payee,
                    "Description": desc,
                    "Category": category,
                    "Property": stessa_property,
                    "Unit": ""
                })

    # Write CSV
    fieldnames = ["Date", "Amount", "Payee", "Description", "Category", "Property", "Unit"]
    with open(output_file, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    
    print(f"Generated Stessa export with {len(rows)} records: {output_file}")

if __name__ == "__main__":
    input_dir = "processed_statements"
    if len(sys.argv) > 1:
        input_dir = sys.argv[1]
    
    generate_stessa_csv(input_dir=input_dir)
