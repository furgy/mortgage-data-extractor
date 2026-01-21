import csv
import os

STESSA_CSV = "Stessa_Transactions-2025.csv"
PB_CSV = "Property_Boss_Transactions-2025.csv"
OUTPUT_CSV = "pb_merged-2025.csv"

# Heuristic mapping function
def map_transaction(gl_account, memo):
    gl_account = gl_account.lower()
    memo = memo.lower()
    
    # Income
    if "rent income" in gl_account:
        return "Rents"
    if "late fee" in gl_account:
        return "Late Fees"
    if "utility reimbursement" in gl_account:
        return "Tenant Pass-Throughs"
    if "eviction fee reimbursement" in gl_account:
        return "Eviction Fees"
    
    # Management
    if "management fees" in gl_account:
        return "Property Management"
    if "leasing fee" in gl_account or "lease renewal fee" in gl_account:
        return "Leasing Commissions"
    
    # Expenses
    if "labor costs" in gl_account:
        return "Labor"
    if "cleaning and maintenance" in gl_account:
        return "Cleaning & Janitorial"
    if "legal and professional fees" in gl_account:
        return "Legal"
    if "material" in gl_account:
        if any(kw in memo for kw in ["plumb", "faucet", "bath", "drain", "sink", "toilet"]):
            return "Plumbing Repairs"
        if "roof" in memo:
            return "Roof Repairs"
        if any(kw in memo for kw in ["lawn", "garden", "tree", "grass", "yard"]):
            return "Gardening & Landscaping"
        if any(kw in memo for kw in ["lock", "key", "door", "screen"]):
            return "Security, Locks & Keys"
        if any(kw in memo for kw in ["paint", "supplies", "moulding", "outlet", "plate", "batteries", "light", "filter", "gloves", "nails"]):
            return "Labor" 
        return "UNCLEAR"

    if "utilities" in gl_account:
        if any(kw in memo for kw in ["water", "sewer", "gsd", "sanitary", "mcd"]):
            return "Water & Sewer"
        if any(kw in memo for kw in ["electric", "firstenergy", "light"]):
            return "Electric"
        if "gas" in memo:
            return "Gas"
        if "nipsco" in memo:
            return "Gas & Electric"
        return "Water & Sewer" # Common default for non-specified utility bills
    
    if "rental registration" in gl_account or "admin fee rental registration" in gl_account:
        return "R&M Permits & Inspections"
    
    # Transfers / Equity
    if "owner contribution" in gl_account:
        return "Owner Contributions"
    if "owner draw" in gl_account:
        return "Owner Distributions"
    
    # Liabilities
    if "security deposit liability" in gl_account:
        return "Security Deposits"
    if "prepayments" in gl_account:
        return "UNCLEAR"

    return "UNCLEAR"

def main():
    if not os.path.exists(PB_CSV):
        print(f"Error: {PB_CSV} not found.")
        return

    with open(PB_CSV, mode='r', newline='', encoding='utf-8-sig') as infile:
        reader = csv.reader(infile)
        header = next(reader)
        
        # Find index of combinedGLAccountName (column 10, index 9)
        try:
            gl_idx = header.index("combinedGLAccountName")
            memo_idx = header.index("postingMemo")
        except ValueError as e:
            print(f"Error: Required columns not found. {e}")
            return
        
        # Insert new column header
        new_header = header[:gl_idx+1] + ["Stessa Mapped Sub-Category"] + header[gl_idx+1:]
        
        output_rows = []
        unclear_count = 0
        total_count = 0
        
        for row in reader:
            if not row: continue
            total_count += 1
            gl_account = row[gl_idx]
            memo = row[memo_idx]
            
            mapped_cat = map_transaction(gl_account, memo)
            if mapped_cat == "UNCLEAR":
                unclear_count += 1
            
            new_row = row[:gl_idx+1] + [mapped_cat] + row[gl_idx+1:]
            output_rows.append(new_row)
            
    with open(OUTPUT_CSV, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(new_header)
        writer.writerows(output_rows)
        
    print(f"Successfully processed {total_count} records.")
    print(f"Mappings completed. {total_count - unclear_count} success, {unclear_count} UNCLEAR.")
    print(f"Output saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
