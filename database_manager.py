import csv
import yaml
from pathlib import Path
import os
from schema import init_db, StessaRaw, PropertyBossRaw, MortgageRaw, Property, CostarRaw
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
                        'details': 'Details'
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

if __name__ == "__main__":
    engine, Session = init_db()
    session = Session()
    
    stessa_file = 'stessa_import_format.csv'
    pb_file = 'Property_Boss_Transactions-2025.csv'
    
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
