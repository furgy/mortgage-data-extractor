import csv
import yaml
from pathlib import Path
import os
from schema import init_db, StessaRaw, PropertyBossRaw, MortgageRaw, Property, CostarRaw, RealtyMedicsRaw, RenshawRaw, AllstarRaw, MikeMikesRaw
from extractor import extract_mortgage_data

def clean_amount(val):
    if not val:
        return 0.0
    # Handle parentheses for negative numbers
    if isinstance(val, str):
        val = val.strip()
        if val.startswith('(') and val.endswith(')'):
            val = '-' + val[1:-1]
        val = val.replace(',', '').replace('$', '').strip()
    try:
        return float(val)
    except ValueError:
        return 0.0


# Helper to seed properties from Stessa (Additive)
def seed_properties_from_stessa(session, stessa_csv_path):
    print(f"Seeding properties from {stessa_csv_path}...")
    if not os.path.exists(stessa_csv_path):
        return

    # 1. Get existing properties to avoid duplicates
    existing_names = {p.stessa_name for p in session.query(Property).all()}
    
    # 2. Read Stessa CSV for unique property names
    new_props = set()
    with open(stessa_csv_path, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            p_name = row.get('Property')
            if p_name and p_name not in existing_names:
                new_props.add(p_name)
    
    # 3. Add valid new properties
    count = 0
    for name in new_props:
        if not name: continue
        # Try to parse address components if possible, or leave blank for user to fill
        # Stessa name is usually just "Street Name" e.g. "4604 Miller Lane"
        # We can try a basic split? No, better leave for user or specific update.
        p = Property(
            stessa_name=name,
            address_display=name, # Default display to stessa name
            is_pb_managed=True  # Default to PB-managed, user can update if needed
        )
        session.add(p)
        count += 1
        
    session.commit()
    if count > 0:
        print(f"Added {count} new properties to the master table.")
    else:
        print("Master property table is up to date.")

def get_property_id_by_stessa_name(session, name):
    if not name: return None
    p = session.query(Property).filter(Property.stessa_name == name).first()
    return p.id if p else None

def get_property_id_by_loan_number(session, loan_num):
    if not loan_num: return None
    # Flexible matching on loan number (contains)
    p = session.query(Property).filter(Property.mortgage_loan_number.like(f"%{loan_num}%")).first()
    return p.id if p else None

def load_stessa_csv(session, csv_path):
    print(f"Loading Stessa data from {csv_path}...")
    session.query(StessaRaw).delete()
    
    with open(csv_path, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            prop_id = get_property_id_by_stessa_name(session, row.get('Property'))
            
            tx = StessaRaw(
                property_id=prop_id,
                date=row['Date'],
                name=row['Name'],
                notes=row['Notes'],
                details=row.get('Details', ''),
                category=row['Category'],
                sub_category=row['Sub-Category'],
                amount=clean_amount(row['Amount']),
                portfolio=row['Portfolio'],
                property=row['Property'],
                unit=row['Unit'],
                data_source=row['Data Source'],
                account=row['Account'],
                owner=row['Owner'],
                attachments=row['Attachments'],
                is_filtered=False,
                filter_reason=None
            )
            session.add(tx)
            # Apply filtering rules from stessa_filters.yaml
            if not hasattr(load_stessa_csv, "_filters"):
                filter_path = Path('stessa_filters.yaml')
                if filter_path.is_file():
                    with open(filter_path) as f:
                        load_stessa_csv._filters = yaml.safe_load(f).get('filters', [])
                else:
                    load_stessa_csv._filters = []
            for rule in load_stessa_csv._filters:
                # Skip if action not EXCLUDE
                if rule.get('action') != 'EXCLUDE':
                    continue
                match = True
                for key, val in rule.items():
                    if key in ('action', 'reason'):
                        continue
                    
                    # Map filter key to CSV column name (handle hyphen vs underscore)
                    # CSV uses "Sub-Category" but filter YAML uses "sub_category"
                    csv_key_mapping = {
                        'sub_category': 'Sub-Category',
                        'category': 'Category',
                        'name': 'Name',
                        'notes': 'Notes',
                        'details': 'Details',
                        'property': 'Property'
                    }
                    
                    # Try mapped key first, then original key, then capitalized versions
                    possible_keys = [csv_key_mapping.get(key, key), key, 
                                   csv_key_mapping.get(key, key).capitalize() if csv_key_mapping.get(key, key) else None,
                                   key.capitalize()]
                    possible_keys = [k for k in possible_keys if k]  # Remove None values
                    
                    row_val = None
                    for pk in possible_keys:
                        if pk in row:
                            row_val = str(row[pk]).strip()
                            break
                    
                    if row_val is None:
                        # Column not found in CSV, skip this rule
                        match = False
                        break
                    
                    if isinstance(val, (int, float)):
                        try:
                            if float(row_val) != float(val):
                                match = False
                                break
                        except ValueError:
                            match = False
                            break
                    else:
                        # Case-insensitive partial match for string fields
                        if key in ('name', 'category', 'sub_category', 'notes', 'details'):
                            if val.lower() not in row_val.lower():
                                match = False
                                break
                        else:
                            if row_val != str(val):
                                match = False
                                break
                if match:
                    tx.is_filtered = True
                    tx.filter_reason = rule.get('reason', 'Excluded by rule')
                    break
    session.commit()
    print(f"Loaded {session.query(StessaRaw).count()} records into stessa_raw.")

def load_property_boss_csv(session, csv_path):
    print(f"Loading Property Boss data from {csv_path}...")
    session.query(PropertyBossRaw).delete()
    
    # Cache properties for linking
    all_props = session.query(Property).all()
    
    with open(csv_path, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Link to Master Property
            # Logic: Match Building Name against Stessa Name or Street Name
            assigned_pid = None
            pb_building = row.get('buildingName', '').strip().upper()
            
            if pb_building:
                for p in all_props:
                    p_name = (p.stessa_name or '').upper()
                    p_street = (p.street or '').upper()
                    
                    # 1. Exact Name Match (Most reliable)
                    if p_name == pb_building:
                        assigned_pid = p.id
                        break
                    
                    # 2. Stessa Name contained in Building Name (or vice versa? usually Building is full name)
                    # e.g. "4604 Miller Lane" == "4604 Miller Lane"
                    
                    # 3. Street containment (User provided "MILLER LN")
                    # Check if "MILLER LN" is in "4604 MILLER LANE" -> need robust check or assume user knows
                    # Simplistic check:
                    if p_street and len(p_street) > 4 and p_street in pb_building:
                        assigned_pid = p.id
                        break
                        
                    # 4. Try matching "Miller" if p.street is "MILLER LN" and building is "4604 Miller Lane"
                    # But "MILLER LN" won't match "Miller Lane" string-wise.
                    # Let's rely on Stessa Name match first, otherwise try standardizing
                    
                    # If Stessa Name "4604 Miller Lane" is in PB Building "4604 Miller Lane" -> Match
                    if p_name and p_name in pb_building:
                         assigned_pid = p.id
                         break

            tx = PropertyBossRaw(
                property_id=assigned_pid,
                buildingName=row['buildingName'],
                unitNumber=row['unitNumber'],
                entryDate=row['entryDate'],
                glAccountId=row['glAccountId'],
                glAccountName=row['glAccountName'],
                glAccountTypeId=row['glAccountTypeId'],
                glAccountSubTypeId=row['glAccountSubTypeId'],
                glAccountExcludedFromCashBalances=row['glAccountExcludedFromCashBalances'],
                parentGLAccountName=row['parentGLAccountName'],
                combinedGLAccountName=row['combinedGLAccountName'],
                payeeName=row['payeeName'],
                postingMemo=row['postingMemo'],
                buildingReserve=row['buildingReserve'],
                amount=clean_amount(row['amount']),
                currentLiabilities=row['currentLiabilities'],
                additionsToCash=row['additionsToCash'],
                subtractionsFromCash=row['subtractionsFromCash'],
                accountType=row['accountType'],
                accountTypeOrderId=row['accountTypeOrderId'],
                ownerName=row['ownerName'],
                journalId=row['journalId'],
                journalCodeId=row['journalCodeId'],
                attributeId=row['attributeId'],
                buildingType=row['buildingType'],
                countryId=row['countryId'],
                addressLine1=row['addressLine1'],
                addressLine2=row['addressLine2'],
                addressLine3=row['addressLine3'],
                city=row['city'],
                state=row['state'],
                zipCode=row['zipCode'],
                buildingId=row['buildingId'],
                unitId=row['unitId'],
                rentalOwnerId=row['rentalOwnerId'],
                buildingStatusId=row['buildingStatusId'],
                showTenantLiabilities=row['showTenantLiabilities'],
                unpaidBillAmount=row['unpaidBillAmount'],
                pendingEpayAmount=row['pendingEpayAmount'],
                is_filtered=False,
                filter_reason=None
            )
            session.add(tx)
            # Apply filtering rules from pb_filters.yaml
            if not hasattr(load_property_boss_csv, "_filters"):
                filter_path = Path('pb_filters.yaml')
                if filter_path.is_file():
                    with open(filter_path) as f:
                        load_property_boss_csv._filters = yaml.safe_load(f).get('filters', [])
                else:
                    load_property_boss_csv._filters = []
            for rule in load_property_boss_csv._filters:
                # Skip if action not EXCLUDE
                if rule.get('action') != 'EXCLUDE':
                    continue
                match = True
                for key, val in rule.items():
                    if key in ('action', 'reason'):
                        continue
                    row_val = str(row.get(key, '')).strip()
                    if isinstance(val, (int, float)):
                        try:
                            if float(row_val) != float(val):
                                match = False
                                break
                        except ValueError:
                            match = False
                            break
                    else:
                        if row_val != str(val):
                            match = False
                            break
                if match:
                    tx.is_filtered = True
                    tx.filter_reason = rule.get('reason', 'Excluded by rule')
                    break
    session.commit()
    print(f"Loaded {session.query(PropertyBossRaw).count()} records into property_boss_raw.")

def load_mortgage_statements(session, statements_dir):
    print(f"Loading mortgage statements from {statements_dir}...")
    session.query(MortgageRaw).delete()
    
    count = 0
    for filename in os.listdir(statements_dir):
        if filename.endswith('.pdf'):
            file_path = os.path.join(statements_dir, filename)
            data = extract_mortgage_data(file_path)
            
            if 'error' in data or data.get('document_type') == 'Unknown':
                continue
            
            loan_num = data.get('loan_number')
            prop_id = get_property_id_by_loan_number(session, loan_num)
            
            # Compute component sum and validate against total amount_due
            component_sum = clean_amount(data.get('principal_breakdown')) + \
                             clean_amount(data.get('interest_breakdown')) + \
                             clean_amount(data.get('escrow_breakdown'))
            total_amount = clean_amount(data.get('amount_due'))
            # Allow a small tolerance for rounding differences
            is_valid = abs(component_sum - total_amount) < 0.01
            validation_error = None
            if not is_valid:
                validation_error = f"Component sum ${component_sum:.2f} does not match total ${total_amount:.2f}"

            tx = MortgageRaw(
                property_id=prop_id,
                bank=data.get('bank'),
                property_address=data.get('property_address'),
                statement_date=data.get('statement_date'),
                payment_due_date=data.get('payment_due_date'),
                amount_due=total_amount,
                principal_breakdown=clean_amount(data.get('principal_breakdown')),
                interest_breakdown=clean_amount(data.get('interest_breakdown')),
                escrow_breakdown=clean_amount(data.get('escrow_breakdown')),
                fees_breakdown=clean_amount(data.get('fees_breakdown')),
                outstanding_principal=clean_amount(data.get('outstanding_principal')),
                loan_number=loan_num,
                is_valid=is_valid,
                validation_error=validation_error
            )
            session.add(tx)
            count += 1
            
    session.commit()
    print(f"Loaded {count} mortgage statements into mortgage_raw.")

def normalize_address_for_matching(addr):
    """Normalize address for matching by removing city/state/zip and common variations."""
    if not addr:
        return ""
    
    # Remove trailing city/state/zip pattern (e.g., ", Chandler, AZ, 85286, US")
    import re
    addr = re.sub(r',\s*[A-Za-z\s]+,\s*[A-Z]{2},\s*\d{5}.*$', '', addr)
    
    # Normalize: uppercase, remove extra spaces
    addr = addr.upper().strip()
    addr = re.sub(r'\s+', ' ', addr)
    
    # Normalize common abbreviations
    addr = addr.replace('STREET', 'ST').replace('ROAD', 'RD').replace('AVENUE', 'AVE')
    addr = addr.replace('DRIVE', 'DR').replace('COURT', 'CT').replace('PLACE', 'PL')
    addr = addr.replace('LANE', 'LN').replace('CIRCLE', 'CIR')
    addr = addr.replace('WEST', 'W').replace('EAST', 'E').replace('NORTH', 'N').replace('SOUTH', 'S')
    
    return addr

def get_property_id_by_costar_address(session, costar_address):
    """Match Costar address to Property table."""
    if not costar_address:
        return None
    
    normalized_costar = normalize_address_for_matching(costar_address)
    
    # Try to match against all properties
    all_props = session.query(Property).all()
    
    for prop in all_props:
        # Try matching against stessa_name
        if prop.stessa_name:
            normalized_stessa = normalize_address_for_matching(prop.stessa_name)
            if normalized_stessa and normalized_stessa in normalized_costar or normalized_costar in normalized_stessa:
                return prop.id
        
        # Try matching against address_display
        if prop.address_display:
            normalized_display = normalize_address_for_matching(prop.address_display)
            if normalized_display and normalized_display in normalized_costar or normalized_costar in normalized_display:
                return prop.id
        
        # Try matching against street
        if prop.street:
            normalized_street = normalize_address_for_matching(prop.street)
            if normalized_street and normalized_street in normalized_costar:
                return prop.id
    
    return None

def load_costar_csv(session, csv_path):
    """Load Costar/Apartments.com rent payment data. Only imports 'Payment' type with 'Completed' status."""
    print(f"Loading Costar data from {csv_path}...")
    session.query(CostarRaw).delete()
    
    count = 0
    skipped = 0
    
    with open(csv_path, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Filter: Only import Payment type with Completed status
            if row.get('Type') != 'Payment' or row.get('Status') != 'Completed':
                skipped += 1
                continue
            
            # Match property address
            property_address = row.get('Property', '').strip()
            prop_id = get_property_id_by_costar_address(session, property_address)
            
            tx = CostarRaw(
                property_id=prop_id,
                type=row.get('Type'),
                memo=row.get('Memo', ''),
                status=row.get('Status'),
                initiated_on=row.get('Initiated On', ''),
                completed_on=row.get('Completed On', ''),
                credit_amt=clean_amount(row.get('Credit Amt', '0')),
                debit_amt=clean_amount(row.get('Debit Amt', '0')),
                initiated_by=row.get('Initiated By', ''),
                property_address=property_address,
                unit=row.get('Unit', ''),
                transaction_id=row.get('TransactionID', ''),
                reference_id=row.get('ReferenceID', '')
            )
            session.add(tx)
            count += 1
    
    session.commit()
    print(f"Loaded {count} Costar payment records (skipped {skipped} non-Payment or non-Completed records).")

def parse_realty_medics_csv(csv_path):
    """
    Parse Realty Medics CSV report and extract individual transactions.
    Returns list of transaction dicts with: account_name, transaction_type, month, amount
    """
    transactions = []
    
    # Skip these account names (headers and totals)
    skip_accounts = {
        'Operating Income & Expense', 'Income', 'Expense', 'Total Operating Income',
        'Total Operating Expense', 'NOI - Net Operating Income', 'Total Income',
        'Total Expense', 'Net Income', 'Other Items', 'Net Other Items', 'Cash Flow',
        'Beginning Cash', 'Beginning Cash + Cash Flow', 'Actual Ending Cash'
    }
    
    with open(csv_path, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            account_name = row.get('Account Name', '').strip()
            if not account_name or account_name in skip_accounts:
                continue
            
            # Determine transaction type based on account name
            # Income accounts: Rent Income, Late Fee, Septic (as income)
            # Expense accounts: Management Fee, Repairs, etc.
            transaction_type = "Income" if any(x in account_name for x in ["Rent Income", "Late Fee", "Septic"]) else "Expense"
            
            # Extract monthly amounts
            month_columns = ['Jan 2025', 'Feb 2025', 'Mar 2025', 'Apr 2025', 'May 2025', 'Jun 2025',
                            'Jul 2025', 'Aug 2025', 'Sep 2025', 'Oct 2025', 'Nov 2025', 'Dec 2025']
            
            for month_col in month_columns:
                amount_str = row.get(month_col, '').strip()
                if not amount_str or amount_str == '0.00' or amount_str == '':
                    continue
                
                amount = clean_amount(amount_str)
                if amount == 0:
                    continue
                
                # For expenses, make amount negative
                if transaction_type == "Expense":
                    amount = -abs(amount)
                else:
                    amount = abs(amount)
                
                transactions.append({
                    'account_name': account_name,
                    'transaction_type': transaction_type,
                    'month': month_col,
                    'amount': amount
                })
    
    return transactions

def map_realty_medics_to_stessa_category(account_name):
    """
    Map Realty Medics account names to Stessa categories.
    Returns tuple: (category, sub_category)
    """
    account_lower = account_name.lower()
    
    # Income mappings
    if 'rent income' in account_lower:
        return ('Income', 'Rents')
    if 'late fee' in account_lower:
        return ('Income', 'Late Fees')
    if 'septic' in account_lower:
        return ('Utilities', 'Water & Sewer')
    
    # Expense mappings
    if 'management fee' in account_lower:
        return ('Management Fees', 'Property Management')
    if 'renewal leasing fee' in account_lower or 'leasing fee' in account_lower:
        return ('Management Fees', 'Leasing Commissions')
    if 'accounting fee' in account_lower:
        return ('Legal & Professional', 'Accounting')
    # Check for capital expenses first (before general repairs)
    if 'landscaping' in account_lower and ('new' in account_lower or 'capital' in account_lower or 'install' in account_lower):
        return ('Capital Expenses', 'New Landscaping')
    if 'repair' in account_lower or 'maintenance' in account_lower:
        return ('Repairs & Maintenance', '')
    if 'landscaping' in account_lower or 'lawn' in account_lower:
        return ('Repairs & Maintenance', 'Gardening & Landscaping')
    if 'hvac' in account_lower:
        return ('Repairs & Maintenance', 'HVAC')
    if 'plumbing' in account_lower:
        return ('Repairs & Maintenance', 'Plumbing Repairs')
    
    # Default
    return ('Expenses', 'Other Expenses')

def load_realty_medics_csv(session, csv_path, property_name=None):
    """
    Load Realty Medics CSV data into realty_medics_raw table.
    If property_name is provided, links transactions to that property.
    Otherwise, tries to match from property names in the CSV or uses combined report logic.
    """
    print(f"Loading Realty Medics data from {csv_path}...")
    
    # Clear existing Realty Medics data for this property if property_name is provided
    # Otherwise clear all (for combined reports, we'll reload everything)
    if property_name:
        # Find property
        prop = session.query(Property).filter(Property.stessa_name.ilike(f'%{property_name}%')).first()
        if prop:
            session.query(RealtyMedicsRaw).filter(RealtyMedicsRaw.property_id == prop.id).delete()
    else:
        session.query(RealtyMedicsRaw).delete()
    session.commit()
    
    transactions = parse_realty_medics_csv(csv_path)
    
    # Map property name to property_id
    prop_id = None
    if property_name:
        prop = session.query(Property).filter(Property.stessa_name.ilike(f'%{property_name}%')).first()
        if prop:
            prop_id = prop.id
    
    # Month to date mapping
    month_to_date = {
        'Jan 2025': '2025-01-01',
        'Feb 2025': '2025-02-01',
        'Mar 2025': '2025-03-01',
        'Apr 2025': '2025-04-01',
        'May 2025': '2025-05-01',
        'Jun 2025': '2025-06-01',
        'Jul 2025': '2025-07-01',
        'Aug 2025': '2025-08-01',
        'Sep 2025': '2025-09-01',
        'Oct 2025': '2025-10-01',
        'Nov 2025': '2025-11-01',
        'Dec 2025': '2025-12-01',
    }
    
    count = 0
    for tx in transactions:
        category, sub_category = map_realty_medics_to_stessa_category(tx['account_name'])
        
        rm_tx = RealtyMedicsRaw(
            property_id=prop_id,
            account_name=tx['account_name'],
            transaction_type=tx['transaction_type'],
            transaction_date=month_to_date.get(tx['month'], ''),
            month=tx['month'],
            amount=tx['amount'],
            stessa_category=category,
            stessa_sub_category=sub_category
        )
        session.add(rm_tx)
        count += 1
    
    session.commit()
    print(f"Loaded {count} Realty Medics transactions into realty_medics_raw.")

def parse_renshaw_html(html_path):
    """
    Parse Renshaw HTML report and extract individual transactions.
    Returns list of transaction dicts with: account_name, account_code, transaction_type, month, amount
    """
    from bs4 import BeautifulSoup
    
    transactions = []
    
    with open(html_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
    
    # Find the table with transaction data
    # The report has sections for Income and Expense
    month_headers = ['JAN 25', 'FEB 25', 'MAR 25', 'APR 25', 'MAY 25', 'JUN 25',
                     'JUL 25', 'AUG 25', 'SEP 25', 'OCT 25', 'NOV 25', 'DEC 25']
    
    # Find all tables
    tables = soup.find_all('table')
    
    current_section = None  # "Income" or "Expense"
    
    for table in tables:
        rows = table.find_all('tr')
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if not cells:
                continue
            
            # Check if this is a section header
            first_cell_text = cells[0].get_text(strip=True).upper()
            if 'INCOME' in first_cell_text and 'EXPENSE' not in first_cell_text:
                current_section = "Income"
                continue
            elif 'EXPENSE' in first_cell_text:
                current_section = "Expense"
                continue
            
            if not current_section:
                continue
            
            # Extract account name and code (format: "Account Name&nbsp;CODE")
            account_cell = cells[0].get_text(strip=True)
            if not account_cell or account_cell in ['Account', 'Total']:
                continue
            
            # Split account name and code
            parts = account_cell.split('&nbsp;')
            account_name = parts[0].strip()
            account_code = parts[1].strip() if len(parts) > 1 else ''
            
            # Skip totals and headers
            if 'Total' in account_name or account_name in ['Account', 'Income', 'Expense']:
                continue
            
            # Extract monthly amounts
            for i, month_header in enumerate(month_headers):
                if i + 1 >= len(cells):
                    break
                
                amount_cell = cells[i + 1]
                amount_text = amount_cell.get_text(strip=True)
                
                # Remove $ and commas
                amount_text = amount_text.replace('$', '').replace(',', '').strip()
                
                if not amount_text or amount_text == '0.00' or amount_text == '':
                    continue
                
                try:
                    amount = float(amount_text)
                    if amount == 0:
                        continue
                    
                    # For expenses, make amount negative
                    if current_section == "Expense":
                        amount = -abs(amount)
                    else:
                        amount = abs(amount)
                    
                    transactions.append({
                        'account_name': account_name,
                        'account_code': account_code,
                        'transaction_type': current_section,
                        'month': month_header,
                        'amount': amount
                    })
                except ValueError:
                    continue
    
    return transactions

def map_renshaw_to_stessa_category(account_name):
    """
    Map Renshaw account names to Stessa categories.
    Returns tuple: (category, sub_category)
    """
    account_lower = account_name.lower()
    
    # Income mappings
    if 'rent' in account_lower:
        return ('Income', 'Rents')
    if 'late fee' in account_lower:
        return ('Income', 'Late Fees')
    if 'resident benefit' in account_lower:
        return ('Income', 'Rents')  # Or appropriate sub-category
    
    # Expense mappings
    if 'management fee' in account_lower or 'management' in account_lower:
        return ('Management Fees', 'Property Management')
    if 'maintenance surcharge' in account_lower:
        return ('Repairs & Maintenance', '')
    
    # Default
    return ('Expenses', 'Other Expenses')

def load_renshaw_html(session, html_path, property_name=None):
    """
    Load Renshaw HTML data into renshaw_raw table.
    If property_name is provided, links transactions to that property.
    """
    print(f"Loading Renshaw data from {html_path}...")
    
    # Clear existing Renshaw data
    session.query(RenshawRaw).delete()
    session.commit()
    
    transactions = parse_renshaw_html(html_path)
    
    # Map property name to property_id
    prop_id = None
    if property_name:
        prop = session.query(Property).filter(Property.stessa_name.ilike(f'%{property_name}%')).first()
        if prop:
            prop_id = prop.id
    
    # Month to date mapping (Renshaw uses "JAN 25" format)
    month_to_date = {
        'JAN 25': '2025-01-01',
        'FEB 25': '2025-02-01',
        'MAR 25': '2025-03-01',
        'APR 25': '2025-04-01',
        'MAY 25': '2025-05-01',
        'JUN 25': '2025-06-01',
        'JUL 25': '2025-07-01',
        'AUG 25': '2025-08-01',
        'SEP 25': '2025-09-01',
        'OCT 25': '2025-10-01',
        'NOV 25': '2025-11-01',
        'DEC 25': '2025-12-01',
    }
    
    count = 0
    for tx in transactions:
        category, sub_category = map_renshaw_to_stessa_category(tx['account_name'])
        
        renshaw_tx = RenshawRaw(
            property_id=prop_id,
            account_name=tx['account_name'],
            account_code=tx['account_code'],
            transaction_type=tx['transaction_type'],
            transaction_date=month_to_date.get(tx['month'], ''),
            month=tx['month'],
            amount=tx['amount'],
            stessa_category=category,
            stessa_sub_category=sub_category
        )
        session.add(renshaw_tx)
        count += 1
    
    session.commit()
    print(f"Loaded {count} Renshaw transactions into renshaw_raw.")

def parse_allstar_csv(csv_path):
    """
    Parse Allstar CSV report and extract individual transactions.
    Returns list of transaction dicts with: account_name, transaction_type, month, amount
    """
    transactions = []
    
    # Skip these account names (headers and totals)
    skip_accounts = {
        'Operating Income & Expense', 'Income', 'Expense', 'Total Operating Income',
        'Total Operating Expense', 'NOI - Net Operating Income', 'Total Income',
        'Total Expense', 'Net Income', 'Other Items', 'Net Other Items', 'Cash Flow',
        'Beginning Cash', 'Beginning Cash + Cash Flow', 'Actual Ending Cash', 'Total Repairs',
        'Total Utilities', 'Total Cleaning and Maintenance'
    }
    
    with open(csv_path, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            account_name = row.get('Account Name', '').strip()
            if not account_name or account_name in skip_accounts:
                continue
            
            # Determine transaction type based on account name
            transaction_type = "Income" if any(x in account_name for x in ["Rent Income", "Late Fee", "Utility Income"]) else "Expense"
            
            # Extract monthly amounts
            month_columns = ['Jan 2025', 'Feb 2025', 'Mar 2025', 'Apr 2025', 'May 2025', 'Jun 2025',
                            'Jul 2025', 'Aug 2025', 'Sep 2025', 'Oct 2025', 'Nov 2025', 'Dec 2025']
            
            for month_col in month_columns:
                amount_str = row.get(month_col, '').strip()
                if not amount_str or amount_str == '0.00' or amount_str == '':
                    continue
                
                amount = clean_amount(amount_str)
                if amount == 0:
                    continue
                
                # For expenses, make amount negative
                if transaction_type == "Expense":
                    amount = -abs(amount)
                else:
                    amount = abs(amount)
                
                transactions.append({
                    'account_name': account_name,
                    'transaction_type': transaction_type,
                    'month': month_col,
                    'amount': amount
                })
    
    return transactions

def map_allstar_to_stessa_category(account_name):
    """
    Map Allstar account names to Stessa categories.
    Returns tuple: (category, sub_category)
    """
    account_lower = account_name.lower()
    
    # Income mappings
    if 'rent income' in account_lower:
        return ('Income', 'Rents')
    if 'late fee' in account_lower:
        return ('Income', 'Late Fees')
    if 'utility income' in account_lower:
        return ('Utilities', 'Water & Sewer')
    
    # Expense mappings
    if 'commissions paid' in account_lower:
        return ('Management Fees', 'Property Management')
    if 'water' in account_lower:
        return ('Utilities', 'Water & Sewer')
    if 'gas' in account_lower:
        return ('Utilities', 'Gas')
    if 'landscaping' in account_lower or 'lawn maintenance' in account_lower:
        return ('Repairs & Maintenance', 'Gardening & Landscaping')
    if 'administrative fees' in account_lower:
        return ('Management Fees', 'Booking & Platform Fees')
    if 'utilities surcharge' in account_lower:
        return ('Utilities', '')
    if 'repair' in account_lower or 'maintenance' in account_lower:
        # Check for specific repair types
        if 'security' in account_lower or 'lock' in account_lower or 'key' in account_lower:
            return ('Repairs & Maintenance', 'Security, Locks & Keys')
        return ('Repairs & Maintenance', '')
    if 'hvac' in account_lower:
        return ('Repairs & Maintenance', 'HVAC')
    if 'plumbing' in account_lower:
        return ('Repairs & Maintenance', 'Plumbing Repairs')
    if 'carpet cleaning' in account_lower:
        return ('Repairs & Maintenance', 'Cleaning')
    if 'licenses and permits' in account_lower:
        return ('Admin & Other', 'Licenses')
    
    # Default
    return ('Expenses', 'Other Expenses')

def load_allstar_csv(session, csv_path, property_name=None):
    """
    Load Allstar CSV data into allstar_raw table.
    If property_name is provided, links transactions to that property.
    """
    print(f"Loading Allstar data from {csv_path}...")
    
    # Clear existing Allstar data
    session.query(AllstarRaw).delete()
    session.commit()
    
    transactions = parse_allstar_csv(csv_path)
    
    # Map property name to property_id
    prop_id = None
    if property_name:
        prop = session.query(Property).filter(Property.stessa_name.ilike(f'%{property_name}%')).first()
        if prop:
            prop_id = prop.id
    else:
        # Default to Malacca St if not specified
        prop = session.query(Property).filter(Property.stessa_name.ilike('%malacca%')).first()
        if prop:
            prop_id = prop.id
    
    # Month to date mapping
    month_to_date = {
        'Jan 2025': '2025-01-01',
        'Feb 2025': '2025-02-01',
        'Mar 2025': '2025-03-01',
        'Apr 2025': '2025-04-01',
        'May 2025': '2025-05-01',
        'Jun 2025': '2025-06-01',
        'Jul 2025': '2025-07-01',
        'Aug 2025': '2025-08-01',
        'Sep 2025': '2025-09-01',
        'Oct 2025': '2025-10-01',
        'Nov 2025': '2025-11-01',
        'Dec 2025': '2025-12-01',
    }
    
    count = 0
    for tx in transactions:
        category, sub_category = map_allstar_to_stessa_category(tx['account_name'])
        
        allstar_tx = AllstarRaw(
            property_id=prop_id,
            account_name=tx['account_name'],
            transaction_type=tx['transaction_type'],
            transaction_date=month_to_date.get(tx['month'], ''),
            month=tx['month'],
            amount=tx['amount'],
            stessa_category=category,
            stessa_sub_category=sub_category
        )
        session.add(allstar_tx)
        count += 1
    
    session.commit()
    print(f"Loaded {count} Allstar transactions into allstar_raw.")

def map_mike_mikes_to_stessa_category(description):
    """
    Map Mike & Mikes transaction descriptions to Stessa categories.
    Returns tuple: (category, sub_category)
    """
    desc_lower = description.lower()
    
    # Income mappings
    if 'rent' in desc_lower and 'income' in desc_lower:
        return ('Income', 'Rents')
    if 'late fee' in desc_lower:
        return ('Income', 'Late Fees')
    if 'utility charge' in desc_lower:
        return ('Utilities', 'Water & Sewer')
    
    # Expense mappings
    if 'management fee' in desc_lower:
        return ('Management Fees', 'Property Management')
    if 'snow removal' in desc_lower or 'snow' in desc_lower:
        return ('Repairs & Maintenance', 'Snow Removal')
    if 'landscaping' in desc_lower:
        return ('Repairs & Maintenance', 'Gardening & Landscaping')
    if 'legal' in desc_lower:
        return ('Legal & Professional', 'Legal Fees')
    if 'maint' in desc_lower or 'repair' in desc_lower:
        return ('Repairs & Maintenance', '')
    if 'pest' in desc_lower or 'animal control' in desc_lower:
        return ('Repairs & Maintenance', 'Pest Control')
    if 'utility' in desc_lower and 'expense' in desc_lower:
        return ('Utilities', '')
    
    # Default
    return ('Expenses', 'Other Expenses')

def load_mike_mikes_statements(session, statements_dir):
    """
    Load Mike & Mikes PDF statements from a directory.
    Processes all PDF files in the directory.
    """
    from extractor import MikeMikesExtractor
    
    print(f"Loading Mike & Mikes statements from {statements_dir}...")
    
    # Clear existing Mike & Mikes data
    session.query(MikeMikesRaw).delete()
    session.commit()
    
    # Find property (4708 N 36th St)
    prop = session.query(Property).filter(
        Property.stessa_name.ilike('%36th%') | 
        Property.address_display.ilike('%36th%')
    ).first()
    
    if not prop:
        print("⚠️  Warning: Could not find property for 4708 N 36th St")
        prop_id = None
    else:
        prop_id = prop.id
        print(f"  Linked to property: {prop.stessa_name}")
    
    count = 0
    errors = 0
    
    if not os.path.exists(statements_dir):
        print(f"  Directory {statements_dir} does not exist.")
        return
    
    for filename in os.listdir(statements_dir):
        if not filename.lower().endswith('.pdf'):
            continue
        
        file_path = os.path.join(statements_dir, filename)
        try:
            extractor = MikeMikesExtractor(file_path)
            data = extractor.extract()
            
            if 'error' in data or data.get('document_type') != 'Property Management Statement':
                continue
            
            # Process each transaction
            for tx in data.get('transactions', []):
                category, sub_category = map_mike_mikes_to_stessa_category(tx['description'])
                
                mike_mikes_tx = MikeMikesRaw(
                    property_id=prop_id,
                    statement_date=data.get('statement_date', ''),
                    statement_start=data.get('statement_start', ''),
                    statement_end=data.get('statement_end', ''),
                    description=tx['description'],
                    transaction_date=tx['date'],
                    amount=tx['amount'],
                    transaction_type='Income' if tx.get('is_income', tx['amount'] > 0) else 'Expense',
                    stessa_category=category,
                    stessa_sub_category=sub_category
                )
                session.add(mike_mikes_tx)
                count += 1
            
        except Exception as e:
            print(f"  ⚠️  Error processing {filename}: {e}")
            errors += 1
            continue
    
    session.commit()
    print(f"Loaded {count} Mike & Mikes transactions from {len([f for f in os.listdir(statements_dir) if f.endswith('.pdf')])} PDF(s) (errors: {errors}).")

if __name__ == "__main__":
    engine, Session = init_db()
    session = Session()
    
    stessa_file = 'inputs/stessa_import_format.csv'
    pb_file = 'inputs/Property_Boss_Transactions-2025.csv'
    
    # Seed properties from Stessa (if new ones exist)
    if os.path.exists(stessa_file):
        seed_properties_from_stessa(session, stessa_file)
        load_stessa_csv(session, stessa_file)
    else:
        print(f"File not found: {stessa_file}")
        
    if os.path.exists(pb_file):
        load_property_boss_csv(session, pb_file)
    else:
        print(f"File not found: {pb_file}")

    statements_dir = 'statements'
    if os.path.isdir(statements_dir):
        load_mortgage_statements(session, statements_dir)
    else:
        print(f"Directory not found: {statements_dir}")
    
    session.close()
