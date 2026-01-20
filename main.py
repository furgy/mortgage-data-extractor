import asyncio
import sys
import os
import json
from retriever import retrieve_statement
from extractor import extract_mortgage_data

def load_registry(filepath="statements/downloads.json"):
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            return json.load(f)
    return {}

def save_registry(registry, filepath="statements/downloads.json"):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(registry, f, indent=4)

async def run_pipeline(account_nickname=None, date_text=None, input_dir="statements", output_dir="processed_statements"):
    registry_path = os.path.join(output_dir, "downloads.json")
    registry = load_registry(registry_path)
    os.makedirs(output_dir, exist_ok=True)
    
    # Identify files to process
    files_to_process = []
    if account_nickname and date_text:
        # Single file mode (legacy support)
        filename = f"{account_nickname}_{date_text.replace('/', '').replace(' ', '_')}.pdf"
        filepath = os.path.join(input_dir, filename)
        if os.path.exists(filepath):
            files_to_process.append(filepath)
    else:
        # Batch mode: all PDFs in input_dir
        for f in os.listdir(input_dir):
            if f.lower().endswith(".pdf"):
                files_to_process.append(os.path.join(input_dir, f))

    if not files_to_process:
        print(f"No PDF files found in {input_dir}")
        return

    for filepath in files_to_process:
        print(f"\n--- Identifying: {os.path.basename(filepath)} ---")
        
        # 1. Extract data to get property and date
        data = extract_mortgage_data(filepath)
        if data.get("error"):
            print(f"Error processing {filepath}: {data['error']}")
            continue

        doc_type = data.get("document_type", "Unknown")
        if doc_type != "Mortgage Statement":
            print(f"Skipped: Detected type '{doc_type}'. Not a Mortgage Statement.")
            continue

        print(f"Confirmed: Mortgage Statement for {data.get('property_address')}")
        addr = data.get("filename_safe_address", "Unknown_Address")
        date = data.get("formatted_date", "00000000")
        registry_key = f"{addr}_{date}"

        # 2. Check registry for duplicates
        if registry_key in registry:
            print(f"Skipping: {addr} ({date}) already in registry.")
            continue

        # 3. Rename and move to processed_statements
        final_filename = f"{addr}-{date}.pdf"
        final_path = os.path.join(output_dir, final_filename)
        
        # Copy file to final location (preserving original for safety)
        import shutil
        shutil.copy2(filepath, final_path)
        print(f"Saved: {final_filename}")

        # 4. Save JSON alongside
        json_path = final_path.replace(".pdf", ".json")
        with open(json_path, "w") as f:
            json.dump(data, f, indent=4)
        
        # 5. Update Registry
        import datetime
        registry[registry_key] = {
            "original_file": os.path.basename(filepath),
            "property_address": data.get("property_address"),
            "statement_date": data.get("statement_date"),
            "processed_timestamp": datetime.datetime.now().isoformat(),
            "file_path": final_path
        }
        save_registry(registry, registry_path)

    print(f"\nBatch processing complete. Results in {output_dir}/")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Mortgage Data Extraction Pipeline")
    parser.add_argument("--account", help="Account nickname (for single file mode)")
    parser.add_argument("--date", help="Date text (for single file mode)")
    parser.add_argument("--input", default="statements", help="Directory containing PDF statements")
    parser.add_argument("--output", default="processed_statements", help="Directory for processed results")
    
    args = parser.parse_args()
    
    if args.account and args.date:
        asyncio.run(run_pipeline(args.account, args.date, args.input, args.output))
    else:
        # Default to batch mode
        asyncio.run(run_pipeline(input_dir=args.input, output_dir=args.output))
