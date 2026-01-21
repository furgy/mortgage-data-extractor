import datetime
import os
import argparse
from schema import init_db, StessaRaw, PropertyBossRaw, MortgageRaw, ReconciliationMatch, Property, CostarRaw
import re

# ... (rest of imports/functions)

def parse_date(date_str):
    if not date_str:
        return None
    formats = ['%m/%d/%Y', '%Y-%m-%d', '%m/%d/%y', '%d-%b-%Y']
    for fmt in formats:
        try:
            return datetime.datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


def filter_by_year(records, date_field, year):
    """
    Filter records by year based on a date field.
    
    Args:
        records: List of database records
        date_field: String name of the date field (e.g., 'date', 'entryDate', 'statement_date')
        year: Integer year to filter by, or None to return all records
    
    Returns:
        Filtered list of records
    """
    if year is None:
        return records
    
    filtered = []
    for record in records:
        date_str = getattr(record, date_field, None)
        if date_str:
            date_obj = parse_date(date_str)
            if date_obj and date_obj.year == year:
                filtered.append(record)
    
    return filtered


def run_reconciliation(year=None):
    engine, Session = init_db()
    session = Session()
    
    # RELOAD DATA to ensure latest CSV changes are reflected
    # This assumes database_manager.py's load_properties() logic is sufficient
    # We might want to re-run the full loader here or assume user ran it. 
    # For robust workflow, let's call the loader functions again? 
    # Or just assume DB is fresh from valid previous step. 
    # Let's re-run loaders to be safe as per previous pattern.
    print("Reloading Stessa and Property Boss data...")
    from database_manager import seed_properties_from_stessa, load_stessa_csv, load_property_boss_csv, load_mortgage_statements, load_costar_csv
    
    stessa_file = 'inputs/stessa_import_format.csv'
    if os.path.exists(stessa_file):
        seed_properties_from_stessa(session, stessa_file)
        load_stessa_csv(session, stessa_file)
    
    if os.path.exists('inputs/Property_Boss_Transactions-2025.csv'):
        load_property_boss_csv(session, 'inputs/Property_Boss_Transactions-2025.csv')
        
    # We should also reload mortgage statements to ensure linking happens
    # But that might be slow if there are many PDFs. 
    # Let's assumemortgage statements are already loaded via main pipeline or previous step 
    # UNLESS we want to force re-link.
    # Given the user flow, let's assume DB is populated but Stessa/PB might change more often.
    # Actually, if we changed properties.csv, we MUST reload everything to re-link.
    if os.path.exists('statements'):
        load_mortgage_statements(session, 'statements')
    
    # Load Costar rent payments if available
    if os.path.exists('inputs/costar-payment-data.csv'):
        load_costar_csv(session, 'inputs/costar-payment-data.csv')

    # Clear previous matches
    session.query(ReconciliationMatch).delete()
    
    # Query all records (exclude filtered transactions)
    stessa_txs = session.query(StessaRaw).filter(StessaRaw.is_filtered == False).all()
    pb_txs = session.query(PropertyBossRaw).filter(PropertyBossRaw.is_filtered == False).all()
    mortgage_stmts = session.query(MortgageRaw).all()
    costar_txs = session.query(CostarRaw).all()
    
    # Filter by year if specified
    if year:
        stessa_txs = filter_by_year(stessa_txs, 'date', year)
        pb_txs = filter_by_year(pb_txs, 'entryDate', year)
        mortgage_stmts = filter_by_year(mortgage_stmts, 'statement_date', year)
        costar_txs = filter_by_year(costar_txs, 'completed_on', year)
        print(f"Filtering reconciliation to year {year}...")
    
    # Sort statements by date
    mortgage_stmts.sort(key=lambda x: parse_date(x.statement_date) or datetime.date.min)
    
    year_label = f" for {year}" if year else ""
    print(f"Starting reconciliation{year_label}: {len(stessa_txs)} Stessa, {len(pb_txs)} PB, {len(mortgage_stmts)} Mortgage, {len(costar_txs)} Apartments.com...")
    
    matched_stessa_ids = set()
    matched_pb_ids = set()
    matches_count = 0
    
    # --- PHASE 1: Mortgage Matching (ID-Based) ---
    print("PHASE 1: Matching Mortgage components (Database-Centric)...")
    
    # Pre-group Stessa transactions by Property ID for faster lookup? 
    # Or just filter in loop. Loop is fine for dataset size.
    
    for pass_num in [1, 2]:
        tolerance = 15 if pass_num == 1 else 35
        print(f"  Pass {pass_num} (Tolerance: {tolerance}d)...")
        
        for m_stmt in mortgage_stmts:
            if not m_stmt.property_id:
                continue # Cannot match if not linked to a property
                
            # Use payment_due_date for matching (transactions occur on/around payment due date)
            # Fall back to statement_date if payment_due_date is not available
            m_date = parse_date(m_stmt.payment_due_date) or parse_date(m_stmt.statement_date)
            
            components = [
                ('Principal', m_stmt.principal_breakdown),
                ('Interest', m_stmt.interest_breakdown),
                ('Escrow', m_stmt.escrow_breakdown)
            ]
            
            for comp_name, comp_amount in components:
                if not comp_amount or comp_amount <= 0: continue
                
                # Check existng match
                already_matched = session.query(ReconciliationMatch).filter(
                    ReconciliationMatch.mortgage_id == m_stmt.id,
                    ReconciliationMatch.notes.like(f"Mortgage {comp_name}%")
                ).first()
                if already_matched: continue
                
                potential_matches = []
                
                # FIND CANDIDATES: Same Property ID
                candidates = session.query(StessaRaw).filter(
                    StessaRaw.property_id == m_stmt.property_id
                ).all()
                
                for s_tx in candidates:
                    if s_tx.id in matched_stessa_ids: continue
                    
                    # Category Filter
                    if comp_name == 'Principal' and s_tx.sub_category != 'Mortgage Principal': continue
                    if comp_name == 'Interest' and s_tx.sub_category != 'Mortgage Interest': continue
                    if comp_name == 'Escrow' and s_tx.sub_category != 'General Escrow Payments': continue
                    
                    s_date = parse_date(s_tx.date)
                    if not s_date: continue
                    
                    date_diff = abs((s_date - m_date).days)
                    if date_diff > tolerance: continue
                    
                    # Check amount match
                    amount_match = abs(s_tx.amount + comp_amount) < 0.005
                    amount_diff = abs(s_tx.amount + comp_amount)
                    
                    # Create match even if amounts don't match exactly (flag in notes)
                    potential_matches.append((s_tx, date_diff, amount_match, amount_diff))
                            
                if potential_matches:
                    # Sort by: exact amount match first, then by date difference
                    potential_matches.sort(key=lambda x: (not x[2], x[1]))
                    best_s_tx, best_diff, exact_amount, amount_diff = potential_matches[0]
                    
                    # Create match note indicating if amount is exact or mismatched
                    if exact_amount:
                        match_note = f"Mortgage {comp_name} match (diff={best_diff}d)"
                        match_score = 1.0
                    else:
                        match_note = f"Mortgage {comp_name} match (diff={best_diff}d, AMT MISMATCH: Stmt={comp_amount:.2f} vs Stessa={abs(best_s_tx.amount):.2f})"
                        match_score = 0.5  # Lower score for amount mismatches
                    
                    match = ReconciliationMatch(
                        stessa_id=best_s_tx.id,
                        mortgage_id=m_stmt.id,
                        match_score=match_score,
                        match_type='mortgage_component',
                        notes=match_note
                    )
                    session.add(match)
                    matched_stessa_ids.add(best_s_tx.id)
                    matches_count += 1

    # --- PHASE 2: Property Boss Matching ---
    print("PHASE 2: Matching Property Boss transactions...")
    # TODO: PB matching logic updates? 
    # PB currently loaded with NULL property_id mostly. 
    # Stick to Amount/Date matching for now or try to link PB property IDs later.
    # The existing logic relies on global Amount/Date match which is risky but user focused on Mortgage first.
    # Let's keep existing PB logic but maybe add ID check if available.
    
    for s_tx in stessa_txs:
        if s_tx.id in matched_stessa_ids: continue
        
        # Skip Property Boss matching if this property is not PB-managed
        if s_tx.property_id:
            prop = session.get(Property, s_tx.property_id)
            if prop and prop.is_pb_managed == False:
                continue  # Skip matching for non-PB-managed properties
        
        s_date = parse_date(s_tx.date)
        s_amount = s_tx.amount
        
        potential_matches = []
        for p_tx in pb_txs:
            if p_tx.id in matched_pb_ids: continue
            
            # If both have property ID, they MUST match
            if s_tx.property_id and p_tx.property_id:
                if s_tx.property_id != p_tx.property_id:
                    continue
            
            p_date = parse_date(p_tx.entryDate)
            p_amount_normalized = -p_tx.amount
            
            if abs(s_amount - p_amount_normalized) < 0.01:
                date_diff = abs((s_date - p_date).days)
                if date_diff <= 4:
                    potential_matches.append((p_tx, date_diff))
        
        if potential_matches:
            potential_matches.sort(key=lambda x: x[1])
            best_p_tx, best_diff = potential_matches[0]
            
            match = ReconciliationMatch(
                stessa_id=s_tx.id,
                pb_id=best_p_tx.id,
                match_score=1.0,
                match_type='amount_date',
                notes=f"PB match: Date diff={best_diff} days"
            )
            session.add(match)
            matched_stessa_ids.add(s_tx.id)
            matched_pb_ids.add(best_p_tx.id)
            matches_count += 1

    # --- PHASE 1.5: Detect Unsplit Mortgage Payments ---
    print("PHASE 1.5: Detecting unsplit mortgage payments...")
    unsplit_mortgages = []
    
    for m_stmt in mortgage_stmts:
        if not m_stmt.property_id:
            continue
        
        # Check if we have all three component matches
        component_matches = session.query(ReconciliationMatch).filter(
            ReconciliationMatch.mortgage_id == m_stmt.id,
            ReconciliationMatch.match_type == 'mortgage_component'
        ).all()
        
        # Count matches by component type
        matched_components = set()
        for match in component_matches:
            if 'Principal' in match.notes:
                matched_components.add('Principal')
            elif 'Interest' in match.notes:
                matched_components.add('Interest')
            elif 'Escrow' in match.notes:
                matched_components.add('Escrow')
        
        # If we don't have all three components matched, check for unsplit payment
        if len(matched_components) < 3 and m_stmt.amount_due:
            # Use payment_due_date for matching (transactions occur on/around payment due date)
            m_date = parse_date(m_stmt.payment_due_date) or parse_date(m_stmt.statement_date)
            total_amount = m_stmt.amount_due
            
            # Look for a Stessa transaction matching the total amount
            # that is NOT one of the expected component categories
            candidates = session.query(StessaRaw).filter(
                StessaRaw.property_id == m_stmt.property_id
            ).all()
            
            for s_tx in candidates:
                if s_tx.id in matched_stessa_ids:
                    continue
                
                # Skip if it's already a component transaction
                if s_tx.sub_category in ['Mortgage Principal', 'Mortgage Interest', 'General Escrow Payments']:
                    continue
                
                s_date = parse_date(s_tx.date)
                if s_date:
                    date_diff = abs((s_date - m_date).days)
                    # Check if amount matches total mortgage payment (within tolerance)
                    # Stessa amounts are negative, so we compare with negative total
                    if date_diff <= 35 and abs(s_tx.amount + total_amount) < 0.01:
                        # Found an unsplit mortgage payment
                        unsplit_mortgages.append({
                            'mortgage': m_stmt,
                            'stessa_tx': s_tx,
                            'date_diff': date_diff,
                            'matched_components': matched_components
                        })
                        break  # Only flag once per mortgage statement
    
    # --- PHASE 3: Apartments.com Rent Payment Matching ---
    print("PHASE 3: Matching Apartments.com rent payments with Stessa income...")
    matched_costar_ids = set()
    
    for costar_tx in costar_txs:
        if not costar_tx.property_id or costar_tx.credit_amt <= 0:
            continue  # Skip if no property match or no credit amount
        
        if costar_tx.id in matched_costar_ids:
            continue
        
        # Use completed_on date for matching (when payment was actually received)
        costar_date = parse_date(costar_tx.completed_on)
        if not costar_date:
            continue
        
        costar_amount = costar_tx.credit_amt  # Rent received (positive)
        
        # Look for matching Stessa income transactions
        potential_matches = []
        
        for s_tx in stessa_txs:
            if s_tx.id in matched_stessa_ids:
                continue
            
            # Must be same property
            if s_tx.property_id != costar_tx.property_id:
                continue
            
            # Must be rent income: Category = "Income" AND (Sub-Category = "Rents" OR payee contains "apartments")
            # Stessa shows rent as category "Income" with sub_category "Rents" and payee like "Apartments.com" or "Apartmentscom Apts..."
            category_is_income = (s_tx.category or '') == 'Income'
            sub_category_is_rents = (s_tx.sub_category or '').lower() == 'rents'
            payee_contains_apartments = 'apartments' in (s_tx.name or '').lower() or 'apartmentscom' in (s_tx.name or '').lower()
            
            # Match if: Income category AND (Rents sub-category OR apartments in payee name)
            if not (category_is_income and (sub_category_is_rents or payee_contains_apartments)):
                continue
            
            s_date = parse_date(s_tx.date)
            if not s_date:
                continue
            
            # Amount match: Costar credit_amt is positive income
            # Stessa amounts can be positive (income) or negative (expenses), but for Rents they should be positive
            # Match the credit amount against Stessa amount (both should be positive for rent income)
            if abs(s_tx.amount - costar_amount) < 0.01:
                date_diff = abs((s_date - costar_date).days)
                if date_diff <= 25:  # 25 day tolerance for rent payments (payments can come in late)
                    potential_matches.append((s_tx, date_diff))
        
        if potential_matches:
            potential_matches.sort(key=lambda x: x[1])
            best_s_tx, best_diff = potential_matches[0]
            
            match = ReconciliationMatch(
                stessa_id=best_s_tx.id,
                costar_id=costar_tx.id,
                match_score=1.0,
                match_type='costar_rent',
                notes=f"Apartments.com rent match: Date diff={best_diff}d, Amount={costar_amount:.2f}"
            )
            session.add(match)
            matched_stessa_ids.add(best_s_tx.id)
            matched_costar_ids.add(costar_tx.id)
            matches_count += 1
    
    session.commit()
    print(f"Reconciliation finished. Total matches: {matches_count}")
    if unsplit_mortgages:
        print(f"Found {len(unsplit_mortgages)} unsplit mortgage payment(s) needing to be split.")
    generate_report(session, unsplit_mortgages, year)
    session.close()

def generate_report(session, unsplit_mortgages=None, year=None):
    if unsplit_mortgages is None:
        unsplit_mortgages = []
    
    print("\n" + "="*50)
    year_label = f" ({year})" if year else ""
    print(f"         RECONCILIATION AUDIT REPORT{year_label}")
    print("="*50)
    
    total_stessa = session.query(StessaRaw).count()
    total_pb = session.query(PropertyBossRaw).count()
    total_mortgage = session.query(MortgageRaw).count()
    total_costar = session.query(CostarRaw).count()
    total_matches = session.query(ReconciliationMatch).count()
    
    # Count filtered transactions if year is specified
    stessa_year_count = None
    pb_year_count = None
    mortgage_year_count = None
    costar_year_count = None
    if year:
        all_stessa = session.query(StessaRaw).all()
        stessa_year_count = len(filter_by_year(all_stessa, 'date', year))
        
        all_pb = session.query(PropertyBossRaw).filter(PropertyBossRaw.is_filtered == False).all()
        pb_year_count = len(filter_by_year(all_pb, 'entryDate', year))
        
        all_mortgage = session.query(MortgageRaw).all()
        mortgage_year_count = len(filter_by_year(all_mortgage, 'statement_date', year))
        
        all_costar = session.query(CostarRaw).all()
        costar_year_count = len(filter_by_year(all_costar, 'completed_on', year))
    
    # Calculate unmatched counts for filtered year
    matched_stessa_ids_all = [m.stessa_id for m in session.query(ReconciliationMatch).all()]
    unmatched_stessa_all = session.query(StessaRaw).filter(
        ~StessaRaw.id.in_(matched_stessa_ids_all),
        StessaRaw.is_filtered == False
    ).all()
    
    if year:
        unmatched_stessa_filtered = filter_by_year(unmatched_stessa_all, 'date', year)
        unmatched_count = len(unmatched_stessa_filtered)
    else:
        unmatched_count = len(unmatched_stessa_all)
    
    print(f"Total Stessa Transactions: {total_stessa}" + (f" ({stessa_year_count} - in {year})" if year and stessa_year_count is not None else ""))
    print(f"Total PB Transactions:     {total_pb}" + (f" ({pb_year_count} - in {year})" if year and pb_year_count is not None else ""))
    print(f"Total Mortgage Statements: {total_mortgage} (x3 components)" + (f" ({mortgage_year_count} - in {year})" if year and mortgage_year_count is not None else ""))
    print(f"Total Apartments.com Payments: {total_costar}" + (f" ({costar_year_count} - in {year})" if year and costar_year_count is not None else ""))
    print(f"Successfully Matched:      {total_matches}")
    if year:
        print(f"Unmatched Transactions ({year}): {unmatched_count}")
    print("-" * 50)
    
    # Matched Mortgage components
    mortgage_matches = session.query(ReconciliationMatch).filter(ReconciliationMatch.match_type == 'mortgage_component').count()
    print(f"Mortgage Component Matches: {mortgage_matches} / {total_mortgage * 3} (expected)")

    # Unmatched Stessa (exclude filtered transactions)
    matched_stessa_ids = [m.stessa_id for m in session.query(ReconciliationMatch).all()]
    unmatched_stessa = session.query(StessaRaw).filter(
        ~StessaRaw.id.in_(matched_stessa_ids),
        StessaRaw.is_filtered == False
    ).all()
    
    # Apply year filter if specified
    if year:
        unmatched_stessa = filter_by_year(unmatched_stessa, 'date', year)
    
    print(f"\nUNMATCHED STESSA (Top 15 by amount):")
    unmatched_stessa.sort(key=lambda x: abs(x.amount), reverse=True)
    
    # Print header with aligned columns
    print(f"  {'Date':12} | {'Amount':>10} | {'Payee':25} | {'Category':20} | {'Property':20}")
    print(f"  {'-'*12}-|-{'-'*10}-|-{'-'*25}-|-{'-'*20}-|-{'-'*20}")
    
    for tx in unmatched_stessa[:15]:
        # Format date consistently as mm/dd/yyyy
        tx_date = parse_date(tx.date)
        if tx_date:
            formatted_date = tx_date.strftime('%m/%d/%Y')
        else:
            formatted_date = tx.date or "N/A"
        
        category = tx.category or "N/A"
        property_name = tx.property or ""
        
        print(f"  {formatted_date:12} | {tx.amount:10.2f} | {tx.name[:25]:25} | {category[:20]:20} | {property_name[:20]:20}")
    
    # Items that are Income/Management but unmatched with Property Boss
    # Only show for PB-managed properties (non-PB-managed properties won't have PB matches)
    unmatched_income_management = []
    for tx in unmatched_stessa:
        if 'Management' in tx.category or 'Income' in tx.category:
            # Only include if property is PB-managed (skip non-PB-managed properties)
            skip = False
            if tx.property_id:
                prop = session.get(Property, tx.property_id)
                if prop and prop.is_pb_managed == False:
                    skip = True  # Skip non-PB-managed properties
            if not skip:
                unmatched_income_management.append(tx)
    
    if unmatched_income_management:
        print(f"\nUNMATCHED INCOME/MANAGEMENT TRANSACTIONS ({len(unmatched_income_management)}):")
        print("  ℹ️  These Stessa transactions (from PB-managed properties) weren't matched with Property Boss.")
        print("      They may need manual review or may be valid transactions without PB equivalents.")
        print(f"  {'Date':12} | {'Amount':>10} | {'Category':15} | {'Property':20}")
        print(f"  {'-'*12}-|-{'-'*10}-|-{'-'*15}-|-{'-'*20}")
        for tx in unmatched_income_management[:15]:
             print(f"  {tx.date:12} | {tx.amount:10.2f} | {tx.category:15} | {tx.property[:20]}")
        if len(unmatched_income_management) > 15:
            print(f"  ... and {len(unmatched_income_management) - 15} more")

    # Unmatched PB
    matched_pb_ids = [m.pb_id for m in session.query(ReconciliationMatch).filter(ReconciliationMatch.pb_id.isnot(None)).all()]
    unmatched_pb = session.query(PropertyBossRaw).filter(
        ~PropertyBossRaw.id.in_(matched_pb_ids),
        PropertyBossRaw.is_filtered == False
    ).all()
    
    # Apply year filter if specified
    if year:
        unmatched_pb = filter_by_year(unmatched_pb, 'entryDate', year)
    
    if unmatched_pb:
        print(f"\nUNMATCHED PROPERTY BOSS (Missing in Stessa):")
        unmatched_pb.sort(key=lambda x: abs(x.amount), reverse=True)
        
        # Print header with aligned columns
        print(f"  {'Date':12} | {'Amount':>10} | {'Payee':25} | {'Category':20} | {'Property':20}")
        print(f"  {'-'*12}-|-{'-'*10}-|-{'-'*25}-|-{'-'*20}-|-{'-'*20}")
        
        for tx in unmatched_pb[:15]:
            # Format date consistently as mm/dd/yyyy
            tx_date = parse_date(tx.entryDate)
            if tx_date:
                formatted_date = tx_date.strftime('%m/%d/%Y')
            else:
                formatted_date = tx.entryDate or "N/A"
            
            hint = ""
            if tx.property_id:
                prop = session.get(Property, tx.property_id)
                hint = f" (Linked to: {prop.stessa_name})"
            
            # Truncate building name if needed
            building_name = tx.buildingName or ""
            building_display = f"{building_name[:20]}{hint}"
            if len(building_display) > 43:
                building_display = building_display[:40] + "..."
            
            # For unmatched PB transactions, infer Stessa category from Property Boss GL account mapping
            # This uses the same logic as map_pb_to_stessa.py
            def map_pb_to_stessa_category(gl_account, memo):
                """Map Property Boss GL account to Stessa category"""
                if not gl_account:
                    return None
                gl_account = str(gl_account).lower()
                memo = str(memo or '').lower()
                
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
                    return "Repairs & Maintenance"
                
                if "utilities" in gl_account:
                    if any(kw in memo for kw in ["water", "sewer", "gsd", "sanitary", "mcd"]):
                        return "Water & Sewer"
                    if any(kw in memo for kw in ["electric", "firstenergy", "light"]):
                        return "Electric"
                    if "gas" in memo:
                        return "Gas"
                    return "Utilities"
                
                return None
            
            # Try to infer category from GL account mapping first
            category = map_pb_to_stessa_category(tx.combinedGLAccountName, tx.postingMemo)
            
            # If mapping didn't work, try to find a potential Stessa match with similar amount/date
            if not category and tx.property_id:
                p_date = parse_date(tx.entryDate)
                p_amount_normalized = -tx.amount  # PB amounts need normalization
                
                if p_date:
                    # Look for nearby Stessa transactions with similar amounts
                    potential_stessa = session.query(StessaRaw).filter(
                        StessaRaw.property_id == tx.property_id,
                        StessaRaw.is_filtered == False
                    ).all()
                    
                    for s_tx in potential_stessa:
                        s_date = parse_date(s_tx.date)
                        if s_date and abs((s_date - p_date).days) <= 30:  # Wider tolerance for category suggestion
                            if abs(s_tx.amount - p_amount_normalized) < 0.01:
                                category = s_tx.category or None
                                break
            
            if not category:
                category = "N/A"
            
            print(f"  {formatted_date:12} | {tx.amount:10.2f} | {tx.payeeName[:25]:25} | {category[:20]:20} | {building_display[:20]:20}")

    if total_matches < 370: # Arbitrary check for demonstration if needed
         pass

    # Unmatched Mortgage
    matched_mort_ids = [m.mortgage_id for m in session.query(ReconciliationMatch).filter(ReconciliationMatch.mortgage_id.isnot(None)).all()]
    unmatched_mort = session.query(MortgageRaw).filter(~MortgageRaw.id.in_(matched_mort_ids)).all()
    
    # Apply year filter if specified
    if year:
        unmatched_mort = filter_by_year(unmatched_mort, 'statement_date', year)
    
    # Flagged Mortgages
    flagged_mortgages = session.query(MortgageRaw).filter(MortgageRaw.is_valid == False).all()
    
    # Apply year filter if specified
    if year:
        flagged_mortgages = filter_by_year(flagged_mortgages, 'statement_date', year)
    if flagged_mortgages:
        print(f"\nFLAGGED MORTGAGE DISCREPANCIES ({len(flagged_mortgages)}):")
        for m in flagged_mortgages:
            print(f"  Statement: {m.statement_date} | {m.bank:10} | {m.property_address[:25]}")
            print(f"  Status:    {m.validation_error}")
            print(f"  {'Component':15} | {'Statement':10} | {'Stessa':10} | {'Status'}")
            print(f"  {'-'*15}-|-{'-'*10}-|-{'-'*10}-|-{'-'*6}")
            
            # Find matches for this specific statement
            m_matches = session.query(ReconciliationMatch).filter(ReconciliationMatch.mortgage_id == m.id).all()
            comp_map = {
                'Principal': (m.principal_breakdown, 'Mortgage Principal'),
                'Interest': (m.interest_breakdown, 'Mortgage Interest'),
                'Escrow': (m.escrow_breakdown, 'General Escrow Payments')
            }
            
            for name, (val, subcat) in comp_map.items():
                s_tx_val = "MISSING"
                status = "MISMATCH"
                
                if val is None: val = 0.0
                found_match = False
                for match in m_matches:
                    s_tx = session.query(StessaRaw).filter(StessaRaw.id == match.stessa_id).first()
                    if s_tx and s_tx.sub_category == subcat:
                        s_tx_val = f"{abs(s_tx.amount):.2f}"
                        if abs(abs(s_tx.amount) - val) < 0.005:
                            status = "MATCH"
                        found_match = True
                        break
                
                # Near miss logic based on ID now, simpler
                if not found_match and m.property_id:
                     candidates = session.query(StessaRaw).filter(
                        StessaRaw.property_id == m.property_id,
                        StessaRaw.sub_category == subcat
                     ).all()
                     for s_tx in candidates:
                        s_date = parse_date(s_tx.date)
                        # Use payment_due_date for matching (transactions occur on/around payment due date)
                        m_date = parse_date(m.payment_due_date) or parse_date(m.statement_date)
                        if m_date and abs((s_date - m_date).days) <= 35:
                             s_tx_val = f"{abs(s_tx.amount):.2f} ({s_tx.date})"
                             status = "MISMATCH (Amt)"
                             break

                print(f"  {name:15} | {val:10.2f} | {s_tx_val:>10} | {status}")
            print("")

    # Mortgage Component Amount Mismatches
    # Find mortgage statements where components were matched but amounts don't match
    amount_mismatch_mortgages = []
    all_mortgage_stmts = session.query(MortgageRaw).all()
    
    # Apply year filter if specified
    if year:
        all_mortgage_stmts = filter_by_year(all_mortgage_stmts, 'statement_date', year)
    
    for m_stmt in all_mortgage_stmts:
        if not m_stmt.property_id:
            continue
        
        # Get all component matches for this mortgage
        component_matches = session.query(ReconciliationMatch).filter(
            ReconciliationMatch.mortgage_id == m_stmt.id,
            ReconciliationMatch.match_type == 'mortgage_component'
        ).all()
        
        if not component_matches:
            continue  # Skip if no matches at all
        
        # Check each component for amount mismatches
        component_details = []
        has_mismatch = False
        
        comp_map = {
            'Principal': (m_stmt.principal_breakdown, 'Mortgage Principal'),
            'Interest': (m_stmt.interest_breakdown, 'Mortgage Interest'),
            'Escrow': (m_stmt.escrow_breakdown, 'General Escrow Payments')
        }
        
        for comp_name, (stmt_amount, subcat) in comp_map.items():
            if not stmt_amount or stmt_amount <= 0:
                continue
            
            # Find the match for this component
            component_match = None
            for match in component_matches:
                if comp_name in match.notes:
                    component_match = match
                    break
            
            if component_match:
                s_tx = session.query(StessaRaw).filter(StessaRaw.id == component_match.stessa_id).first()
                if s_tx:
                    stessa_amount = abs(s_tx.amount)
                    amount_diff = abs(stessa_amount - stmt_amount)
                    is_match = amount_diff < 0.005
                    
                    if not is_match:
                        has_mismatch = True
                    
                    component_details.append({
                        'name': comp_name,
                        'statement_amount': stmt_amount,
                        'stessa_amount': stessa_amount,
                        'stessa_date': s_tx.date,
                        'amount_diff': amount_diff,
                        'is_match': is_match
                    })
            else:
                # Component not matched at all
                component_details.append({
                    'name': comp_name,
                    'statement_amount': stmt_amount,
                    'stessa_amount': None,
                    'stessa_date': None,
                    'amount_diff': None,
                    'is_match': False
                })
                has_mismatch = True
        
        if has_mismatch:
            amount_mismatch_mortgages.append({
                'mortgage': m_stmt,
                'components': component_details
            })
    
    if amount_mismatch_mortgages:
        # Sort by date (ascending), then by property address
        amount_mismatch_mortgages.sort(key=lambda x: (
            parse_date(x['mortgage'].statement_date) or datetime.date.max,
            x['mortgage'].property_address or ""
        ))
        
        print(f"\nMORTGAGE COMPONENT AMOUNT MISMATCHES ({len(amount_mismatch_mortgages)}):")
        print("  ⚠️  These mortgage statements have matching component transactions in Stessa,")
        print("      but the amounts don't match the statement breakdown")
        print("")
        
        # Track grand totals across all mortgages
        grand_stmt_total = 0.0
        grand_stessa_total = 0.0
        grand_total_diff = 0.0
        
        for item in amount_mismatch_mortgages:
            m = item['mortgage']
            hint = ""
            if m.property_id:
                prop = session.get(Property, m.property_id)
                hint = f" (Linked to: {prop.stessa_name})"
            
            print(f"  Statement: {m.statement_date} | {m.bank:10} | {m.property_address[:25]}{hint}")
            print(f"  Total Amount Due: ${m.amount_due:.2f}")
            print(f"  {'Component':15} | {'Stmt Amt':>12} | {'Stessa Amt':>12} | {'Difference':>12} | {'Stessa Date':12} | {'Status'}")
            print(f"  {'-'*15}-|-{'-'*12}-|-{'-'*12}-|-{'-'*12}-|-{'-'*12}-|-{'-'*6}")
            
            # Calculate totals for this mortgage
            stmt_total = 0.0
            stessa_total = 0.0
            
            for comp in item['components']:
                stmt_total += comp['statement_amount'] or 0.0
                if comp['stessa_amount'] is not None:
                    stessa_total += comp['stessa_amount']
                    stessa_amt_str = f"${comp['stessa_amount']:.2f}"
                    diff_str = f"${comp['amount_diff']:.2f}"
                    status = "MATCH" if comp['is_match'] else "MISMATCH"
                    date_str = comp['stessa_date'] or "N/A"
                else:
                    stessa_amt_str = "NOT FOUND"
                    diff_str = "N/A"
                    status = "MISSING"
                    date_str = "N/A"
                
                print(f"  {comp['name']:15} | ${comp['statement_amount']:>11.2f} | {stessa_amt_str:>12} | {diff_str:>12} | {date_str:12} | {status}")
            
            # Print total line for this mortgage
            total_diff = abs(stessa_total - stmt_total)
            print(f"  {'-'*15}-|-{'-'*12}-|-{'-'*12}-|-{'-'*12}-|-{'-'*12}-|-{'-'*6}")
            print(f"  {'TOTALS':15} | ${stmt_total:>11.2f} | ${stessa_total:>11.2f} | ${total_diff:>11.2f} | {'':12} | {'MISMATCH' if total_diff >= 0.005 else 'MATCH'}")
            print("")
            
            # Accumulate grand totals
            grand_stmt_total += stmt_total
            grand_stessa_total += stessa_total
            grand_total_diff += total_diff
        
        # Print grand total summary
        if len(amount_mismatch_mortgages) > 1:
            print(f"  {'='*15}=|={'='*12}=|={'='*12}=|={'='*12}=|={'='*12}=|={'='*6}")
            print(f"  {'GRAND TOTALS':15} | ${grand_stmt_total:>11.2f} | ${grand_stessa_total:>11.2f} | ${grand_total_diff:>11.2f} | {'':12} | {'MISMATCH' if grand_total_diff >= 0.005 else 'MATCH'}")
            print(f"  {'(' + str(len(amount_mismatch_mortgages)) + ' mortgages)':15} | {'Statement Total':>12} | {'Stessa Total':>12} | {'Total Diff':>12} | {'':12} | {'':6}")
            print("")
    
    # Mortgage Payments Needing Split
    # Filter by year if specified
    if unsplit_mortgages and year:
        unsplit_mortgages = [item for item in unsplit_mortgages 
                            if parse_date(item['mortgage'].statement_date) and 
                            parse_date(item['mortgage'].statement_date).year == year]
    
    if unsplit_mortgages:
        print(f"\nMORTGAGE PAYMENTS NEEDING SPLIT ({len(unsplit_mortgages)}):")
        print("  ⚠️  These mortgage payments appear as single transactions but should be split into")
        print("      three components: Principal, Interest, and Escrow")
        print(f"  {'Statement Date':15} | {'Amount':>10} | {'Bank':10} | {'Property':25} | {'Components Matched'}")
        print(f"  {'-'*15}-|-{'-'*10}-|-{'-'*10}-|-{'-'*25}-|-{'-'*20}")
        
        for item in unsplit_mortgages:
            m = item['mortgage']
            s_tx = item['stessa_tx']
            matched = item['matched_components']
            
            hint = ""
            if m.property_id:
                prop = session.get(Property, m.property_id)
                hint = f" (Linked to: {prop.stessa_name})"
            
            components_str = ", ".join(sorted(matched)) if matched else "None"
            
            prop_display = f"{m.property_address[:20]}{hint}"
            print(f"  {m.statement_date:15} | {m.amount_due:10.2f} | {m.bank:10} | {prop_display[:45]}")
            print(f"    → Stessa TX: {s_tx.date} | {abs(s_tx.amount):10.2f} | {s_tx.sub_category or s_tx.category or 'Unknown'}")
            print(f"    → Components already matched: {components_str}")
            print(f"    → Expected: Principal ({m.principal_breakdown or 0:.2f}), Interest ({m.interest_breakdown or 0:.2f}), Escrow ({m.escrow_breakdown or 0:.2f})")
            print(f"    → Date diff: {item['date_diff']} days")
            print("")
    
    if unmatched_mort:
        print(f"\nUNMATCHED MORTGAGE STATEMENTS (Missing in Stessa):")
        for m in unmatched_mort[:15]:
            # Diagnostic: Check if linked to property
            hint = ""
            if m.property_id:
                prop = session.get(Property, m.property_id)
                hint = f" (Linked to: {prop.stessa_name})"
            else:
                hint = " (Unlinked - Check properties table in DB)"
            
            print(f"  {m.statement_date} | {m.amount_due:10.2f} | {m.bank:10} | {m.property_address[:20]}{hint}")
    
    # Unmatched Costar
    matched_costar_ids = [m.costar_id for m in session.query(ReconciliationMatch).filter(ReconciliationMatch.costar_id.isnot(None)).all()]
    unmatched_costar = session.query(CostarRaw).filter(~CostarRaw.id.in_(matched_costar_ids)).all()
    
    # Apply year filter if specified
    if year:
        unmatched_costar = filter_by_year(unmatched_costar, 'completed_on', year)
    
    if unmatched_costar:
        print(f"\nUNMATCHED APARTMENTS.COM RENT PAYMENTS (Missing in Stessa):")
        print(f"  {'Date':12} | {'Amount':>10} | {'Memo':30} | {'Property':25}")
        print(f"  {'-'*12}-|-{'-'*10}-|-{'-'*30}-|-{'-'*25}")
        
        for tx in unmatched_costar[:15]:
            # Format date consistently as mm/dd/yyyy
            tx_date = parse_date(tx.completed_on)
            if tx_date:
                formatted_date = tx_date.strftime('%m/%d/%Y')
            else:
                formatted_date = tx.completed_on or "N/A"
            
            property_display = tx.property_address or "Unknown"
            # Truncate property address if needed
            if len(property_display) > 25:
                property_display = property_display[:22] + "..."
            
            memo = tx.memo or ""
            if len(memo) > 30:
                memo = memo[:27] + "..."
            
            hint = ""
            if tx.property_id:
                prop = session.get(Property, tx.property_id)
                if prop:
                    hint = f" (Linked to: {prop.stessa_name})"
            
            print(f"  {formatted_date:12} | {tx.credit_amt:10.2f} | {memo:30} | {property_display[:25]}{hint}")
        
        if len(unmatched_costar) > 15:
            print(f"  ... and {len(unmatched_costar) - 15} more")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Reconcile mortgage, Stessa, and Property Boss transactions')
    parser.add_argument('--year', type=int, default=2025, 
                        help='Calendar year to reconcile (default: 2025). Use --year 0 to reconcile all years.')
    args = parser.parse_args()
    
    # Convert 0 to None for "all years"
    year = args.year if args.year != 0 else None
    
    run_reconciliation(year=year)
