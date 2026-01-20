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
