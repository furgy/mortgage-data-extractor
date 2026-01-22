import re
import json
import sys
import os
from pypdf import PdfReader

class BaseExtractor:
    def __init__(self, pdf_path):
        self.pdf_path = pdf_path
        self.reader = PdfReader(pdf_path)
        self.all_text = self._extract_all_text()
        self.first_page_text = self.reader.pages[0].extract_text(extraction_mode="layout")

    def _extract_all_text(self):
        text = ""
        for page in self.reader.pages:
            text += page.extract_text(extraction_mode="layout") + "\n"
        return text

    def clean_currency(self, val):
        if val is None: return "0.00"
        return val.replace('$', '').replace(',', '').strip()

    def get_filename_safe_address(self, addr):
        if not addr: return "Unknown_Address"
        clean = re.sub(r'[,.]', '', addr)
        clean = re.sub(r'\s+', '_', clean)
        return clean

    def get_yyyy_mm_dd(self, date_str):
        if not date_str: return "00000000"
        m = re.match(r'(\d{2})/(\d{2})/(\d{4})', date_str)
        if m:
            return f"{m.group(3)}{m.group(1)}{m.group(2)}"
        return date_str.replace('/', '')

    def validate_data(self, data):
        try:
            comp_fields = ["principal_breakdown", "interest_breakdown", "escrow_breakdown", "fees_breakdown"]
            components = [float(data.get(f, "0").replace(',','') or 0) for f in comp_fields]
            total_calc = sum(components)
            total_extracted = float(data.get("amount_due", "0").replace(',','') or 0)

            if abs(total_calc - total_extracted) < 0.01:
                data["validation_status"] = "valid"
            else:
                data["validation_status"] = "invalid"
                data["validation_error"] = f"Sum of components ({total_calc:.2f}) does not match Total Amount Due ({total_extracted:.2f})"
        except Exception as e:
            data["validation_status"] = "warning"
            data["validation_error"] = f"Could not perform validation: {str(e)}"
        return data

    def extract(self):
        raise NotImplementedError("Subclasses must implement extract()")

class HuntingtonExtractor(BaseExtractor):
    def extract(self):
        text = self.all_text
        patterns = {
            "loan_number": r"Loan Account Number\s*(\d+)",
            "statement_date": r"Statement Date:\s*(\d{2}/\d{2}/\d{4})",
            "payment_due_date": r"Payment DueDate\s*(\d{2}/\d{2}/\d{4})",
            "amount_due": r"Amount Due:\s*\$([\d,]+\.\d{2})",
            "outstanding_principal": r"OutstandingPrincipal\s*\$([\d,]+\.\d{2})",
            "maturity_date": r"MaturityDate\s*([a-zA-Z]+\d{4})",
            "interest_rate": r"InterestRate\(.*?\)\s*([\d.]+\%)",
            "principal_breakdown": r"Principal\s*\$([\d,]+\.\d{2})",
            "interest_breakdown": r"Interest\s*\$([\d,]+\.\d{2})",
            "escrow_breakdown": r"Escrow\(fortaxes and\/orinsurance\)\s*\$([\d,]+\.\d{2})",
            "fees_breakdown": r"TotalFees andCharges\s*\$([\d,]+\.\d{2})",
        }
        
        data = {"bank": "Huntington", "document_type": "Mortgage Statement"}
        for key, pattern in patterns.items():
            match = re.search(pattern, text)
            if match:
                val = match.group(1).strip()
                data[key] = self.clean_currency(val) if "breakdown" in key or key in ["amount_due", "outstanding_principal"] else val
            else:
                data[key] = None

        # Address Extraction Logic (Legacy Huntington)
        address_lines = []
        is_address_section = False
        for line in text.split('\n'):
            if "PropertyAddress" in line:
                is_address_section = True
                addr_part = line.split("PropertyAddress")[-1].strip()
                addr_part = re.split(r'\s{5,}', addr_part)[0].strip()
                if addr_part: address_lines.append(addr_part)
                continue
            if is_address_section:
                if any(label in line for label in ["OutstandingPrincipal", "MaturityDate", "InterestRate", "PrepaymentPenalty"]):
                    is_address_section = False
                    continue
                stripped = line.strip()
                if not stripped: continue
                parts = re.split(r'\s{5,}', line.strip())
                addr_part = parts[0]
                if any(x in addr_part for x in ["Principal", "Interest", "Escrow", "RegularMonthly", "TotalFees"]):
                    if line.find(addr_part) < 40:
                        is_address_section = False
                        continue
                if addr_part and len(address_lines) < 3:
                    address_lines.append(addr_part)

        full_address = " ".join(address_lines).strip()
        full_address = re.sub(r'(\d+)([A-Z])', r'\1 \2', full_address)
        full_address = re.sub(r'([A-Z]+)([A-Z]{2})(\d{5})', r'\1 \2 \3', full_address)
        suffixes = ["LN", "RD", "ST", "AVE", "DR", "CT", "PL", "WAY", "TER", "CIR", "BLVD"]
        for suffix in suffixes:
            full_address = re.sub(rf'([A-Z]+)({suffix})\b', r'\1 \2', full_address)
        
        # Cleanup Huntington specific messy merges (e.g. GARYIN)
        full_address = re.sub(r'\s+', ' ', full_address).strip()
        
        data["property_address"] = full_address
        data["filename_safe_address"] = self.get_filename_safe_address(data["property_address"])
        data["formatted_date"] = self.get_yyyy_mm_dd(data["statement_date"])
        
        return self.validate_data(data)

class PNCExtractor(BaseExtractor):
    def extract(self):
        text = self.all_text
        # PNC layout refinement
        patterns = {
            "loan_number": r"Account Number\s+(\d+)",
            "statement_date": r"Statement Date\s+(\d{2}/\d{2}/\d{4})",
            "payment_due_date": r"Payment Due Date\s+(\d{2}/\d{2}/\d{4})",
            "amount_due": r"Amount Due\s+\$([\d,]+\.\d{2})",
            "outstanding_principal": r"Outstanding Principal\s+\$([\d,]+\.\d{2})",
            "interest_rate": r"Interest Rate\s+([\d.]+\%)",
            "principal_breakdown": r"(?<!Outstanding )Principal\s+\$([\d,]+\.\d{2})", # Avoid Outstanding Principal
            "interest_breakdown": r"Interest\s+\$([\d,]+\.\d{2})",
            "escrow_breakdown": r"Escrow \(Taxes and Insurance\)\s+\$([\d,]+\.\d{2})",
        }
        
        data = {"bank": "PNC", "document_type": "Mortgage Statement"}
        for key, pattern in patterns.items():
            # Special logic for principal_breakdown to avoid "Outstanding Principal"
            if key == "principal_breakdown":
                section = re.split(r"Explanation of Amount Due", text)
                if len(section) > 1:
                    # Look for Principal with a large gap before it (indicating right column)
                    breakdown_match = re.search(r"\s{20,}Principal\s{5,}\$([\d,]+\.\d{2})", section[1])
                    if breakdown_match:
                        data[key] = self.clean_currency(breakdown_match.group(1))
                        continue
            
            match = re.search(pattern, text)
            if match:
                val = match.group(1).strip()
                data[key] = self.clean_currency(val) if "breakdown" in key or key in ["amount_due", "outstanding_principal"] else val
            else:
                data[key] = data.get(key, "0.00" if "breakdown" in key else None)

        # PNC Property Address refinement
        # Detect PropertyAddress: and capture until EscrowBalance or PaymentOptions
        addr_match = re.search(r"PropertyAddress:\s*(.*?)(?=EscrowBalance|PaymentOptions|$)", text.replace("\n", " "))
        full_address = addr_match.group(1).strip() if addr_match else "Unknown_Address"
        
        # Clean address merging (CHANDLERAZ -> CHANDLER AZ)
        full_address = re.sub(r'([a-z])([A-Z])', r'\1 \2', full_address)
        full_address = re.sub(r'(\d+)([A-Z])', r'\1 \2', full_address)
        full_address = re.sub(r'([A-Z]+)([A-Z]{2})(\d{5})', r'\1 \2 \3', full_address)
        # FLAMINGODR -> FLAMINGO DR
        suffixes = ["LN", "RD", "ST", "AVE", "DR", "CT", "PL", "WAY", "TER", "CIR", "BLVD"]
        for suffix in suffixes:
            full_address = re.sub(rf'([A-Z]+)({suffix})\b', r'\1 \2', full_address)
        
        # Remove extra internal spaces
        full_address = re.sub(r'\s+', ' ', full_address).strip()
        
        data["property_address"] = full_address
        data["filename_safe_address"] = self.get_filename_safe_address(data["property_address"])
        data["formatted_date"] = self.get_yyyy_mm_dd(data["statement_date"])
        
        # Fees: In PNC, it's often 0 if not listed.
        data["fees_breakdown"] = "0.00"
        
        return self.validate_data(data)

class MikeMikesExtractor(BaseExtractor):
    """Extractor for Mike & Mikes property management statements."""
    
    def extract(self):
        text = self.all_text
        
        data = {
            "document_type": "Property Management Statement",
            "property_manager": "Mike & Mikes"
        }
        
        # Extract statement period (format: "12-01-2025 to 12-31-2025")
        period_match = re.search(r'(\d{2}-\d{2}-\d{4})\s+to\s+(\d{2}-\d{2}-\d{4})', text)
        if period_match:
            data["statement_start"] = period_match.group(1)
            data["statement_end"] = period_match.group(2)
        
        # Extract statement date (format: "01-16-2026")
        date_match = re.search(r'Statement Date\s*(\d{2}-\d{2}-\d{4})', text)
        if date_match:
            data["statement_date"] = date_match.group(1)
        
        # Extract property address
        address_match = re.search(r'(\d+\s+N\s+\d+\w*\s+St[^,]*,\s+Milwaukee[^)]*)', text)
        if address_match:
            data["property_address"] = address_match.group(1).strip()
        
        # Extract transactions from TRANSACTION DETAILS section
        transactions = []
        if 'TRANSACTION DETAILS' in text:
            details_start = text.find('TRANSACTION DETAILS')
            details_end = text.find('OPEN WORK ORDERS', details_start)
            if details_end == -1:
                details_end = text.find('COMPLETED WORK ORDERS', details_start)
            if details_end == -1:
                details_end = details_start + 3000
            
            details_section = text[details_start:details_end]
            
            # Parse transaction lines
            lines = details_section.split('\n')
            current_date = None
            
            # Use statement start date as default if available
            default_date = None
            if data.get('statement_start'):
                # Convert statement_start from MM-DD-YYYY to MM-DD-YYYY format (already in correct format)
                default_date = data.get('statement_start')
            
            for i, line in enumerate(lines):
                line = line.strip()
                if not line:
                    continue
                
                # Skip headers
                if 'TRANSACTION DETAILS' in line or 'Description' in line or 'Date' in line:
                    continue
                
                # Extract beginning/ending balance
                if 'Beginning Balance' in line:
                    bal_match = re.search(r'\$([\d,]+\.\d{2})', line)
                    if bal_match:
                        data["beginning_balance"] = self.clean_currency(bal_match.group(1))
                    continue
                
                if 'Ending Balance' in line:
                    bal_match = re.search(r'\$([\d,]+\.\d{2})', line)
                    if bal_match:
                        data["ending_balance"] = self.clean_currency(bal_match.group(1))
                    continue
                
                # Skip property address lines and reserve info
                if 'Reserve:' in line or 'N 36th St' in line or 'Milwaukee' in line:
                    continue
                
                # Skip summary lines
                if 'Net $' in line or 'Statement Net' in line:
                    continue
                
                # Check if line contains a date (MM-DD-YYYY format)
                date_match = re.search(r'(\d{2}-\d{2}-\d{4})', line)
                if date_match:
                    current_date = date_match.group(1)
                
                # Look for transaction descriptions with amounts
                # Pattern: Description followed by date and amounts
                # Try to find lines with transaction descriptions
                # Use current_date if set, otherwise try default_date for transactions with amounts but no date
                date_to_use = current_date
                if not date_to_use:
                    # Check if this line has amounts - if so, use default_date
                    amounts_check = re.findall(r'\$([\d,]+\.\d{2})', line)
                    if amounts_check and default_date:
                        date_to_use = default_date
                
                if date_to_use:
                    # Look for lines that have a description and amounts
                    # Check if this line or next line has amounts
                    amounts = re.findall(r'\$([\d,]+\.\d{2})', line)
                    
                    if amounts:
                        # Extract description (everything before the first $)
                        desc_part = line.split('$')[0].strip()
                        
                        # Skip if it's just a date or balance
                        if not desc_part or desc_part == current_date or 'Balance' in desc_part:
                            continue
                        
                        # Check if description is on previous line
                        if i > 0 and not amounts and len(lines[i-1].strip()) > 0:
                            prev_line = lines[i-1].strip()
                            if not re.search(r'\$', prev_line) and not re.search(r'\d{2}-\d{2}-\d{4}', prev_line):
                                desc_part = prev_line
                        
                        # Determine transaction type and amount
                        description = desc_part
                        
                        # Check if we have both increase and decrease columns
                        # Look at the structure: Description | Date | Increase | Decrease | Balance
                        # If we have multiple amounts, first non-zero is likely the transaction amount
                        transaction_amount = None
                        is_income = False
                        
                        # Check description for income indicators
                        if any(x in description.lower() for x in ['rent', 'income', 'late fee', 'utility charge']):
                            is_income = True
                            # Find the first non-zero amount (should be increase)
                            for amt in amounts:
                                if amt != '0.00':
                                    transaction_amount = float(self.clean_currency(amt))
                                    break
                        else:
                            # Expense - find decrease amount
                            # Usually the second amount if there are two, or the amount if there's one
                            if len(amounts) >= 2:
                                # Second amount is usually decrease
                                if amounts[1] != '0.00':
                                    transaction_amount = -float(self.clean_currency(amounts[1]))
                            elif len(amounts) == 1:
                                if amounts[0] != '0.00':
                                    transaction_amount = -float(self.clean_currency(amounts[0]))
                        
                        if transaction_amount and transaction_amount != 0:
                            # Skip summary lines
                            if description.lower() in ['net', 'statement net', 'ending balance', 'beginning balance']:
                                continue
                            
                            # Convert date from MM-DD-YYYY to YYYY-MM-DD
                            date_parts = date_to_use.split('-')
                            if len(date_parts) == 3:
                                formatted_date = f"{date_parts[2]}-{date_parts[0]}-{date_parts[1]}"
                            else:
                                formatted_date = date_to_use
                            
                            # Clean up description (remove extra whitespace and date if it's in there)
                            description = re.sub(r'\s+', ' ', description).strip()
                            description = re.sub(r'\d{2}-\d{2}-\d{4}', '', description).strip()
                            
                            transactions.append({
                                "description": description,
                                "date": formatted_date,
                                "amount": transaction_amount,
                                "is_income": is_income
                            })
        
        data["transactions"] = transactions
        return data

def extract_mortgage_data(pdf_path):
    try:
        reader = PdfReader(pdf_path)
        first_page = reader.pages[0].extract_text()
        
        # Routing Logic
        if "Huntington" in first_page:
            extractor = HuntingtonExtractor(pdf_path)
            # Check for Escrow Analysis
            if "EscrowAccountDisclosureStatement" in first_page.replace(" ", ""):
                return {"document_type": "Escrow Analysis", "bank": "Huntington"}
            return extractor.extract()
        elif "PNC" in first_page or "PNC" in reader.pages[1].extract_text():
            extractor = PNCExtractor(pdf_path)
            return extractor.extract()
        else:
            return {"document_type": "Unknown", "error": "Provider not recognized"}
            
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 extractor.py <pdf_path>")
        sys.exit(1)
    
    pdf_file = sys.argv[1]
    result = extract_mortgage_data(pdf_file)
    print(json.dumps(result, indent=4))
