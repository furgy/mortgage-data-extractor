import datetime
import os
import argparse
from itertools import combinations
from schema import init_db, StessaRaw, PropertyBossRaw, MortgageRaw, ReconciliationMatch, Property, CostarRaw, RealtyMedicsRaw, RenshawRaw, AllstarRaw, MikeMikesRaw
import re

# ... (rest of imports/functions)

def matches_management_fee_subcategory(stessa_sub_category, source_sub_category):
    """
    Standardized function to check if sub-categories match for Management Fees.
    Management Fees can have sub-categories like "Property Management" or "Leasing Commissions".
    Returns True if sub-categories match, False otherwise.
    
    Rules:
    - If either is empty, consider it a match (empty means default/unspecified Property Management)
    - If both have values, they must match exactly (case-insensitive)
    - "Property Management" matches "Property Management"
    - "Leasing Commissions" matches "Leasing Commissions"
    """
    stessa_sub = (stessa_sub_category or '').strip().lower()
    source_sub = (source_sub_category or '').strip().lower()
    
    # If either is empty, consider it a match (empty = default Property Management)
    if not stessa_sub or not source_sub:
        return True
    
    # Both have values - must match exactly
    return stessa_sub == source_sub

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


def run_reconciliation(year=None, clear_manual=False):
    engine, Session = init_db()
    session = Session()
    
    # RELOAD DATA to ensure latest CSV changes are reflected
    # This assumes database_manager.py's load_properties() logic is sufficient
    # We might want to re-run the full loader here or assume user ran it. 
    # For robust workflow, let's call the loader functions again? 
    # Or just assume DB is fresh from valid previous step. 
    # Let's re-run loaders to be safe as per previous pattern.
    print("Reloading Stessa and Property Boss data...")
    from database_manager import seed_properties_from_stessa, load_stessa_csv, load_property_boss_csv, load_mortgage_statements, load_costar_csv, load_realty_medics_csv, load_renshaw_html, load_allstar_csv, load_mike_mikes_statements
    
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
    
    # Load Realty Medics income/expense reports if available
    # Clear existing Realty Medics data first to avoid duplicates
    session.query(RealtyMedicsRaw).delete()
    session.commit()
    
    # Try individual property files first, then fall back to combined report
    marion_oaks_file = 'inputs/marion_oaks-2025.csv'
    sw38th_file = 'inputs/sw_38th-2025.csv'
    combined_file = 'inputs/realty_medics_2025.csv'
    
    if os.path.exists(marion_oaks_file):
        load_realty_medics_csv(session, marion_oaks_file, property_name='Marion Oaks')
    if os.path.exists(sw38th_file):
        load_realty_medics_csv(session, sw38th_file, property_name='38th')
    # Only load combined file if individual files don't exist
    if not os.path.exists(marion_oaks_file) and not os.path.exists(sw38th_file) and os.path.exists(combined_file):
        load_realty_medics_csv(session, combined_file)
    
    # Load Renshaw HTML report if available
    renshaw_file = 'inputs/Renshaw-Income- 2025.html'
    if os.path.exists(renshaw_file):
        load_renshaw_html(session, renshaw_file, property_name='Lone Rock')
    
    # Load Allstar CSV report if available
    allstar_file = 'inputs/allstar_2025.csv'
    if os.path.exists(allstar_file):
        load_allstar_csv(session, allstar_file, property_name='Malacca')
    
    # Load Mike & Mikes PDF statements if available
    mike_mikes_dir = 'inputs/mike_mikes'
    if os.path.exists(mike_mikes_dir):
        load_mike_mikes_statements(session, mike_mikes_dir)

    # Clear previous matches (but preserve manual reconciliations unless explicitly cleared)
    if clear_manual:
        session.query(ReconciliationMatch).delete()
    else:
        session.query(ReconciliationMatch).filter(
            ReconciliationMatch.match_type != 'manual_reconciled'
        ).delete()
    
    # Query all records (exclude filtered transactions)
    # Get Stessa transactions, but include "Transfers/Owner Distributions" even if filtered
    # (Owner distributions from property managers should be reconciled)
    stessa_txs = session.query(StessaRaw).filter(
        (StessaRaw.is_filtered == False) |
        ((StessaRaw.category == 'Transfers') & (StessaRaw.sub_category == 'Owner Distributions'))
    ).all()
    pb_txs = session.query(PropertyBossRaw).filter(PropertyBossRaw.is_filtered == False).all()
    mortgage_stmts = session.query(MortgageRaw).all()
    costar_txs = session.query(CostarRaw).all()
    realty_medics_txs = session.query(RealtyMedicsRaw).all()
    renshaw_txs = session.query(RenshawRaw).all()
    allstar_txs = session.query(AllstarRaw).all()
    mike_mikes_txs = session.query(MikeMikesRaw).all()
    
    # Filter by year if specified
    if year:
        stessa_txs = filter_by_year(stessa_txs, 'date', year)
        pb_txs = filter_by_year(pb_txs, 'entryDate', year)
        # For mortgage statements, filter by payment_due_date (with fallback to statement_date)
        # This includes statements issued in Dec 2024 with payment due dates in Jan 2025
        mortgage_stmts = [m for m in mortgage_stmts if (
            (parse_date(m.payment_due_date) and parse_date(m.payment_due_date).year == year) or
            (not m.payment_due_date and parse_date(m.statement_date) and parse_date(m.statement_date).year == year)
        )]
        costar_txs = filter_by_year(costar_txs, 'completed_on', year)
        realty_medics_txs = filter_by_year(realty_medics_txs, 'transaction_date', year)
        renshaw_txs = filter_by_year(renshaw_txs, 'transaction_date', year)
        allstar_txs = filter_by_year(allstar_txs, 'transaction_date', year)
        mike_mikes_txs = filter_by_year(mike_mikes_txs, 'transaction_date', year)
        print(f"Filtering reconciliation to year {year}...")
    
    # Sort statements by date
    mortgage_stmts.sort(key=lambda x: parse_date(x.statement_date) or datetime.date.min)
    
    year_label = f" for {year}" if year else ""
    print(f"Starting reconciliation{year_label}: {len(stessa_txs)} Stessa, {len(pb_txs)} PB, {len(mortgage_stmts)} Mortgage, {len(costar_txs)} Apartments.com, {len(realty_medics_txs)} Realty Medics, {len(renshaw_txs)} Renshaw, {len(allstar_txs)} Allstar, {len(mike_mikes_txs)} Mike & Mikes...")
    
    matched_stessa_ids = set()
    matched_pb_ids = set()
    matches_count = 0
    
    # --- PHASE 1: Mortgage Matching (ID-Based) ---
    print("PHASE 1: Matching Mortgage components (Database-Centric)...")
    
    # Pre-group Stessa transactions by Property ID for faster lookup? 
    # Or just filter in loop. Loop is fine for dataset size.
    
    for pass_num in [1, 2]:
        tolerance = 10 if pass_num == 1 else 15
        print(f"  Pass {pass_num} (Tolerance: {tolerance}d after due date)...")
        
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
                    # If sub_category is empty, allow matching to any component (match by amount)
                    # Otherwise, require exact sub-category match
                    s_sub_cat = (s_tx.sub_category or '').strip()
                    if s_sub_cat:
                        # Has sub-category - must match exactly
                        if comp_name == 'Principal' and s_sub_cat != 'Mortgage Principal': continue
                        if comp_name == 'Interest' and s_sub_cat != 'Mortgage Interest': continue
                        if comp_name == 'Escrow' and s_sub_cat != 'General Escrow Payments': continue
                    # If sub_category is empty, we'll match by amount below
                    
                    s_date = parse_date(s_tx.date)
                    if not s_date: continue
                    
                    # CRITICAL: Transaction must be ON or AFTER payment due date
                    # Transactions before the due date are for previous statement periods
                    if s_date < m_date:
                        continue  # Skip transactions before payment due date
                    
                    # Calculate days AFTER payment due date (not absolute difference)
                    date_diff = (s_date - m_date).days
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
        
        # Only check for unsplit payment if we don't have all three components matched
        # Individual component matches take priority
        if len(matched_components) < 3 and m_stmt.amount_due:
            # Use payment_due_date for matching (transactions occur on/around payment due date)
            m_date = parse_date(m_stmt.payment_due_date) or parse_date(m_stmt.statement_date)
            total_amount = m_stmt.amount_due
            
            # Look for a Stessa transaction matching the total amount
            # that is NOT one of the expected component categories
            candidates = session.query(StessaRaw).filter(
                StessaRaw.property_id == m_stmt.property_id
            ).all()
            
            best_match = None
            best_date_diff = 999
            best_amount_diff = 999
            
            for s_tx in candidates:
                if s_tx.id in matched_stessa_ids:
                    continue
                
                # Skip if it's already a component transaction
                if s_tx.sub_category in ['Mortgage Principal', 'Mortgage Interest', 'General Escrow Payments']:
                    continue
                
                s_date = parse_date(s_tx.date)
                if not s_date:
                    continue
                
                date_diff = abs((s_date - m_date).days)
                # Use 10-day tolerance for unsplit payments (not 30 days)
                if date_diff > 10:
                    continue
                
                # Check if amount matches total mortgage payment
                # Stessa amounts are negative, so we compare with negative total
                amount_diff = abs(s_tx.amount + total_amount)
                # Allow up to $3 tolerance for unsplit payments (fat-finger errors)
                if amount_diff <= 3.00:
                    # Found a potential unsplit mortgage payment
                    # Keep the best match (closest date, then closest amount)
                    if date_diff < best_date_diff or (date_diff == best_date_diff and amount_diff < best_amount_diff):
                        best_match = s_tx
                        best_date_diff = date_diff
                        best_amount_diff = amount_diff
            
            if best_match:
                # Found an unsplit mortgage payment
                unsplit_mortgages.append({
                    'mortgage': m_stmt,
                    'stessa_tx': best_match,
                    'matched_components': matched_components,
                    'date_diff': best_date_diff,
                    'amount_diff': best_amount_diff
                })
    
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
    
    # --- PHASE 4: Realty Medics Transaction Matching ---
    print("PHASE 4: Matching Realty Medics transactions with Stessa...")
    matched_realty_medics_ids = set()
    
    # Get the two properties for Realty Medics (Marion Oaks and SW 38th Cir)
    marion_oaks_prop = session.query(Property).filter(Property.stessa_name.ilike('%marion%oaks%')).first()
    sw38th_prop = session.query(Property).filter(Property.stessa_name.ilike('%38th%')).first()
    realty_medics_properties = [p for p in [marion_oaks_prop, sw38th_prop] if p]
    
    for rm_tx in realty_medics_txs:
        if rm_tx.id in matched_realty_medics_ids:
            continue
        
        # Use transaction_date for matching
        rm_date = parse_date(rm_tx.transaction_date)
        if not rm_date:
            continue
        
        rm_amount = rm_tx.amount  # Can be positive (income) or negative (expense)
        
        # Must match category and sub-category
        rm_category = (rm_tx.stessa_category or '').strip()
        rm_sub_category = (rm_tx.stessa_sub_category or '').strip()
        
        # Determine which properties to check
        # If property_id is set, only check that property
        # Otherwise, check both Realty Medics properties (combined report)
        properties_to_check = []
        if rm_tx.property_id:
            prop = session.get(Property, rm_tx.property_id)
            if prop:
                properties_to_check = [prop]
        else:
            properties_to_check = realty_medics_properties
        
        if not properties_to_check:
            continue
        
        # First, try exact single transaction match
        single_match = None
        for prop in properties_to_check:
            for s_tx in stessa_txs:
                if s_tx.id in matched_stessa_ids:
                    continue
                
                # Must be same property
                if s_tx.property_id != prop.id:
                    continue
                
                # Must match category and sub-category
                stessa_category = (s_tx.category or '').strip()
                stessa_sub_category = (s_tx.sub_category or '').strip()
                
                # Special handling: Capital Expenses can match Repairs & Maintenance for large landscaping projects
                # (Realty Medics may categorize as "Repairs" but Stessa correctly categorizes as "Capital Expenses")
                category_match = False
                if stessa_category.lower() == rm_category.lower():
                    category_match = True
                elif (stessa_category.lower() == 'capital expenses' and 
                      rm_category.lower() == 'repairs & maintenance' and
                      abs(rm_amount) > 1000):  # Large amounts are more likely to be capital expenses
                    category_match = True
                elif (stessa_category.lower() == 'repairs & maintenance' and
                      rm_category.lower() == 'capital expenses' and
                      abs(rm_amount) > 1000):
                    category_match = True
                
                if not category_match:
                    continue
                # For Management Fees, use standardized sub-category matching
                if rm_category.lower() == 'management fees':
                    if not matches_management_fee_subcategory(stessa_sub_category, rm_sub_category):
                        continue
                # For Capital Expenses, allow flexible sub-category matching (e.g., "New Landscaping" vs empty)
                # Also handle when Stessa has Capital Expenses but RM has Repairs & Maintenance
                if stessa_category.lower() == 'capital expenses' or rm_category.lower() == 'capital expenses':
                    # If either sub-category is empty, consider it a match
                    # If both have values, allow partial matches (e.g., "New Landscaping" contains "Landscaping")
                    if rm_sub_category and stessa_sub_category:
                        rm_sub_lower = rm_sub_category.lower()
                        stessa_sub_lower = stessa_sub_category.lower()
                        if (rm_sub_lower != stessa_sub_lower and
                            rm_sub_lower not in stessa_sub_lower and
                            stessa_sub_lower not in rm_sub_lower):
                            continue
                elif rm_category.lower() == 'capital expenses':
                    # If either sub-category is empty, consider it a match
                    # If both have values, allow partial matches (e.g., "New Landscaping" contains "Landscaping")
                    if rm_sub_category and stessa_sub_category:
                        rm_sub_lower = rm_sub_category.lower()
                        stessa_sub_lower = stessa_sub_category.lower()
                        if (rm_sub_lower != stessa_sub_lower and
                            rm_sub_lower not in stessa_sub_lower and
                            stessa_sub_lower not in rm_sub_lower):
                            continue
                # For other categories: if either is empty, consider it a match
                elif rm_sub_category and stessa_sub_category:
                    if stessa_sub_category.lower() != rm_sub_category.lower():
                        continue
                
                s_date = parse_date(s_tx.date)
                if not s_date:
                    continue
                
                # Exact amount match (handle sign differences for utilities - RM shows as income, Stessa as expense)
                # For Utilities/Water & Sewer, compare absolute values since RM shows positive but Stessa shows negative
                if rm_category.lower() == "utilities" and rm_sub_category.lower() == "water & sewer":
                    amount_match = abs(abs(s_tx.amount) - abs(rm_amount)) < 0.01
                else:
                    amount_match = abs(s_tx.amount - rm_amount) < 0.01
                
                if amount_match:
                    date_diff = abs((s_date - rm_date).days)
                    if date_diff <= 30:
                        if single_match is None or date_diff < single_match[2]:
                            single_match = (s_tx, prop, date_diff)
        
        # If exact match found, use it
        if single_match:
            best_s_tx, matched_prop, best_diff = single_match
            match = ReconciliationMatch(
                stessa_id=best_s_tx.id,
                realty_medics_id=rm_tx.id,
                match_score=1.0,
                match_type='realty_medics',
                notes=f"Realty Medics match: {rm_tx.account_name} ({rm_tx.transaction_type}), Property: {matched_prop.stessa_name}, Date diff={best_diff}d, Amount={rm_amount:.2f}"
            )
            session.add(match)
            matched_stessa_ids.add(best_s_tx.id)
            matched_realty_medics_ids.add(rm_tx.id)
            matches_count += 1
            continue
        
        # If no exact match, try split payment matching for individual properties
        # This handles cases where multiple Stessa transactions sum to a single Realty Medics transaction
        # (e.g., rent paid in multiple installments)
        found_match = False
        if rm_tx.property_id and len(properties_to_check) == 1:
            # Try to find combination of transactions from the same property that sum to rm_amount
            prop_to_check = properties_to_check[0]
            candidate_txs = []
            for s_tx in stessa_txs:
                if s_tx.id in matched_stessa_ids:
                    continue
                
                if s_tx.property_id != prop_to_check.id:
                    continue
                
                stessa_category = (s_tx.category or '').strip()
                stessa_sub_category = (s_tx.sub_category or '').strip()
                
                if stessa_category.lower() != rm_category.lower():
                    continue
                # For Management Fees, use standardized sub-category matching
                if rm_category.lower() == 'management fees':
                    if not matches_management_fee_subcategory(stessa_sub_category, rm_sub_category):
                        continue
                # For other categories: if either is empty, consider it a match
                elif rm_sub_category and stessa_sub_category:
                    if stessa_sub_category.lower() != rm_sub_category.lower():
                        continue
                
                s_date = parse_date(s_tx.date)
                if not s_date:
                    continue
                
                date_diff = abs((s_date - rm_date).days)
                if date_diff <= 30:
                    # For Utilities/Water & Sewer, allow opposite signs (RM shows as income, Stessa as expense)
                    # For other categories, require same sign
                    if rm_category.lower() == "utilities" and rm_sub_category.lower() == "water & sewer":
                        candidate_txs.append((s_tx, date_diff))
                    elif (s_tx.amount * rm_amount) > 0:  # Same sign
                        candidate_txs.append((s_tx, date_diff))
            
            # Try combinations of 2-5 transactions (rent can be split into multiple payments)
            if candidate_txs:
                candidate_txs.sort(key=lambda x: x[1])  # Sort by date difference
                
                for combo_size in range(2, min(6, len(candidate_txs) + 1)):
                    for combo in combinations(candidate_txs, combo_size):
                        combo_txs = [tx for tx, _ in combo]
                        if any(tx.id in matched_stessa_ids for tx in combo_txs):
                            continue
                        
                        total_amount = sum(tx.amount for tx in combo_txs)
                        # For Utilities/Water & Sewer, compare absolute values (handle sign differences)
                        if rm_category.lower() == "utilities" and rm_sub_category.lower() == "water & sewer":
                            amount_match = abs(abs(total_amount) - abs(rm_amount)) < 0.01
                        else:
                            amount_match = abs(total_amount - rm_amount) < 0.01
                        
                        if amount_match:
                            # Found a match! Create match records for all transactions
                            primary_tx, primary_diff = combo[0]
                            # Create primary match with full details
                            match = ReconciliationMatch(
                                stessa_id=primary_tx.id,
                                realty_medics_id=rm_tx.id,
                                match_score=0.95,
                                match_type='realty_medics_split',
                                notes=f"Realty Medics split payment match: {rm_tx.account_name} ({rm_tx.transaction_type}), {len(combo_txs)} transactions totaling ${rm_amount:.2f}, Property: {prop_to_check.stessa_name}, Date diff={primary_diff}d"
                            )
                            session.add(match)
                            matched_stessa_ids.add(primary_tx.id)
                            matches_count += 1
                            
                            # Create match records for remaining transactions (link them to the same Realty Medics transaction)
                            for tx, _ in combo[1:]:
                                match = ReconciliationMatch(
                                    stessa_id=tx.id,
                                    realty_medics_id=rm_tx.id,
                                    match_score=0.95,
                                    match_type='realty_medics_split',
                                    notes=f"Realty Medics split payment (part of {len(combo_txs)} transactions totaling ${rm_amount:.2f})"
                                )
                                session.add(match)
                                matched_stessa_ids.add(tx.id)
                                matches_count += 1
                            
                            matched_realty_medics_ids.add(rm_tx.id)
                            found_match = True
                            break
                    
                    if found_match:
                        break
    
    # --- PHASE 5: Renshaw Transaction Matching ---
    print("PHASE 5: Matching Renshaw transactions with Stessa...")
    matched_renshaw_ids = set()
    
    # Get the Lone Rock property
    lone_rock_prop = session.query(Property).filter(Property.stessa_name.ilike('%lone%rock%')).first()
    
    # First, match rent and management fees
    for renshaw_tx in renshaw_txs:
        if renshaw_tx.id in matched_renshaw_ids:
            continue
        
        renshaw_date = parse_date(renshaw_tx.transaction_date)
        if not renshaw_date:
            continue
        
        renshaw_amount = renshaw_tx.amount
        renshaw_category = (renshaw_tx.stessa_category or '').strip()
        renshaw_sub_category = (renshaw_tx.stessa_sub_category or '').strip()
        
        # Must match property
        if not renshaw_tx.property_id or not lone_rock_prop:
            continue
        
        if renshaw_tx.property_id != lone_rock_prop.id:
            continue
        
        # First, try exact single transaction match
        single_match = None
        for s_tx in stessa_txs:
            if s_tx.id in matched_stessa_ids:
                continue
            
            if s_tx.property_id != lone_rock_prop.id:
                continue
            
            stessa_category = (s_tx.category or '').strip()
            stessa_sub_category = (s_tx.sub_category or '').strip()
            
            if stessa_category.lower() != renshaw_category.lower():
                continue
            
            # For Management Fees, use standardized sub-category matching
            if renshaw_category.lower() == 'management fees':
                if not matches_management_fee_subcategory(stessa_sub_category, renshaw_sub_category):
                    continue
            # For other categories: if either is empty, consider it a match
            elif renshaw_sub_category and stessa_sub_category:
                if stessa_sub_category.lower() != renshaw_sub_category.lower():
                    continue
            
            s_date = parse_date(s_tx.date)
            if not s_date:
                continue
            
            # Exact amount match
            if abs(s_tx.amount - renshaw_amount) < 0.01:
                date_diff = abs((s_date - renshaw_date).days)
                if date_diff <= 30:
                    if single_match is None or date_diff < single_match[1]:
                        single_match = (s_tx, date_diff)
        
        # If exact match found, use it
        if single_match:
            best_s_tx, best_diff = single_match
            match = ReconciliationMatch(
                stessa_id=best_s_tx.id,
                renshaw_id=renshaw_tx.id,
                match_score=1.0,
                match_type='renshaw',
                notes=f"Renshaw match: {renshaw_tx.account_name} ({renshaw_tx.transaction_type}), Property: {lone_rock_prop.stessa_name}, Date diff={best_diff}d, Amount={renshaw_amount:.2f}"
            )
            session.add(match)
            matched_stessa_ids.add(best_s_tx.id)
            matched_renshaw_ids.add(renshaw_tx.id)
            matches_count += 1
            continue
        
        # Try split payment matching (multiple Stessa transactions sum to one Renshaw transaction)
        candidate_txs = []
        for s_tx in stessa_txs:
            if s_tx.id in matched_stessa_ids:
                continue
            
            if s_tx.property_id != lone_rock_prop.id:
                continue
            
            stessa_category = (s_tx.category or '').strip()
            stessa_sub_category = (s_tx.sub_category or '').strip()
            
            if stessa_category.lower() != renshaw_category.lower():
                continue
            # For Management Fees, use standardized sub-category matching
            if renshaw_category.lower() == 'management fees':
                if not matches_management_fee_subcategory(stessa_sub_category, renshaw_sub_category):
                    continue
            # For other categories: if either is empty, consider it a match
            elif renshaw_sub_category and stessa_sub_category:
                if stessa_sub_category.lower() != renshaw_sub_category.lower():
                    continue
            
            s_date = parse_date(s_tx.date)
            if not s_date:
                continue
            
            date_diff = abs((s_date - renshaw_date).days)
            if date_diff <= 30:
                if (s_tx.amount * renshaw_amount) > 0:  # Same sign
                    candidate_txs.append((s_tx, date_diff))
        
        # Try combinations of 2-5 transactions
        if candidate_txs:
            candidate_txs.sort(key=lambda x: x[1])
            
            for combo_size in range(2, min(6, len(candidate_txs) + 1)):
                for combo in combinations(candidate_txs, combo_size):
                    combo_txs = [tx for tx, _ in combo]
                    if any(tx.id in matched_stessa_ids for tx in combo_txs):
                        continue
                    
                    total_amount = sum(tx.amount for tx in combo_txs)
                    if abs(total_amount - renshaw_amount) < 0.01:
                        primary_tx, primary_diff = combo[0]
                        # Create primary match with full details
                        match = ReconciliationMatch(
                            stessa_id=primary_tx.id,
                            renshaw_id=renshaw_tx.id,
                            match_score=0.95,
                            match_type='renshaw_split',
                            notes=f"Renshaw split payment match: {renshaw_tx.account_name} ({renshaw_tx.transaction_type}), {len(combo_txs)} transactions totaling ${renshaw_amount:.2f}, Property: {lone_rock_prop.stessa_name}, Date diff={primary_diff}d"
                        )
                        session.add(match)
                        matched_stessa_ids.add(primary_tx.id)
                        matches_count += 1
                        
                        # Create match records for remaining transactions (link them to the same Renshaw transaction)
                        for tx, _ in combo[1:]:
                            match = ReconciliationMatch(
                                stessa_id=tx.id,
                                renshaw_id=renshaw_tx.id,
                                match_score=0.95,
                                match_type='renshaw_split',
                                notes=f"Renshaw split payment (part of {len(combo_txs)} transactions totaling ${renshaw_amount:.2f})"
                            )
                            session.add(match)
                            matched_stessa_ids.add(tx.id)
                            matches_count += 1
                        
                        matched_renshaw_ids.add(renshaw_tx.id)
                        break
                
                if renshaw_tx.id in matched_renshaw_ids:
                    break
        
        # If still no match and it's a management fee, try monthly aggregation
        # (similar to Mike & Mikes - management fees may be split across multiple transactions)
        if renshaw_tx.id not in matched_renshaw_ids and renshaw_category.lower() == 'management fees':
            # Get all transactions in the same month (same year and month)
            month_candidates = []
            for s_tx in stessa_txs:
                if s_tx.id in matched_stessa_ids:
                    continue
                
                if s_tx.property_id != lone_rock_prop.id:
                    continue
                
                stessa_category = (s_tx.category or '').strip()
                stessa_sub_category = (s_tx.sub_category or '').strip()
                
                if stessa_category.lower() != renshaw_category.lower():
                    continue
                
                # For Management Fees, use standardized sub-category matching
                if renshaw_category.lower() == 'management fees':
                    if not matches_management_fee_subcategory(stessa_sub_category, renshaw_sub_category):
                        continue
                # For other categories: sub-category matching
                elif renshaw_sub_category and stessa_sub_category:
                    if stessa_sub_category.lower() != renshaw_sub_category.lower():
                        continue
                
                s_date = parse_date(s_tx.date)
                if not s_date:
                    continue
                
                # Check if same month and year
                if s_date.year == renshaw_date.year and s_date.month == renshaw_date.month:
                    if (s_tx.amount * renshaw_amount) > 0:  # Same sign
                        month_candidates.append(s_tx)
            
            # Sum all month candidates and check if total matches
            if month_candidates:
                # Filter out already matched transactions for the sum calculation
                unmatched_candidates = [tx for tx in month_candidates if tx.id not in matched_stessa_ids]
                if unmatched_candidates:
                    total_month_amount = sum(tx.amount for tx in unmatched_candidates)
                    if abs(total_month_amount - renshaw_amount) < 0.01:
                        # Found a monthly match! Create match records for all unmatched transactions
                        primary_tx = unmatched_candidates[0]
                        # Create primary match with full details
                        match = ReconciliationMatch(
                            stessa_id=primary_tx.id,
                            renshaw_id=renshaw_tx.id,
                            match_score=0.90,
                            match_type='renshaw_monthly',
                            notes=f"Renshaw monthly aggregation match: {renshaw_tx.account_name} ({renshaw_tx.transaction_type}), {len(unmatched_candidates)} transactions in {renshaw_date.strftime('%B %Y')} totaling ${renshaw_amount:.2f}, Property: {lone_rock_prop.stessa_name}"
                        )
                        session.add(match)
                        matched_stessa_ids.add(primary_tx.id)
                        matches_count += 1
                        
                        # Create match records for remaining transactions (link them to the same Renshaw transaction)
                        for tx in unmatched_candidates[1:]:
                            match = ReconciliationMatch(
                                stessa_id=tx.id,
                                renshaw_id=renshaw_tx.id,
                                match_score=0.90,
                                match_type='renshaw_monthly',
                                notes=f"Renshaw monthly aggregation (part of {len(unmatched_candidates)} transactions totaling ${renshaw_amount:.2f})"
                            )
                            session.add(match)
                            matched_stessa_ids.add(tx.id)
                            matches_count += 1
                        
                        matched_renshaw_ids.add(renshaw_tx.id)
    
    # Now match owner distributions (calculated as rent - management fees per month)
    # Group Renshaw transactions by month
    from collections import defaultdict
    renshaw_by_month = defaultdict(lambda: {'rent': None, 'mgmt_fee': None})
    
    for renshaw_tx in renshaw_txs:
        renshaw_date = parse_date(renshaw_tx.transaction_date)
        if not renshaw_date:
            continue
        
        month_key = (renshaw_date.year, renshaw_date.month)
        
        if renshaw_tx.stessa_category == 'Income' and renshaw_tx.stessa_sub_category == 'Rents':
            renshaw_by_month[month_key]['rent'] = renshaw_tx
        elif renshaw_tx.stessa_category == 'Management Fees':
            renshaw_by_month[month_key]['mgmt_fee'] = renshaw_tx
    
    # For each month with both rent and management fee, calculate expected distribution
    for month_key, data in renshaw_by_month.items():
        rent_tx = data['rent']
        mgmt_tx = data['mgmt_fee']
        
        if rent_tx and mgmt_tx:
            # Calculate expected owner distribution: rent - management fee
            expected_distribution = rent_tx.amount + mgmt_tx.amount  # mgmt_tx.amount is negative, so we add
            
            # Look for Stessa transactions matching this distribution amount
            # These are typically "Renshaw Property Sigonfil" payments
            # They can be categorized as "Income/Rents" or "Transfers/Owner Distributions"
            distribution_candidates = []
            for s_tx in stessa_txs:
                if s_tx.id in matched_stessa_ids:
                    continue
                
                if s_tx.property_id != lone_rock_prop.id:
                    continue
                
                # Check if it's a Renshaw-related distribution
                if 'renshaw' not in (s_tx.name or '').lower():
                    continue
                
                # Check if amount matches (within tolerance)
                if abs(s_tx.amount - expected_distribution) < 0.01:
                    s_date = parse_date(s_tx.date)
                    if not s_date:
                        continue
                    
                    # Check if same month
                    if s_date.year == month_key[0] and s_date.month == month_key[1]:
                        # Accept if categorized as Income/Rents or Transfers/Owner Distributions
                        # Owner distributions from property managers may be categorized either way
                        stessa_category = (s_tx.category or '').strip()
                        stessa_sub_category = (s_tx.sub_category or '').strip()
                        if ((stessa_category == 'Income' and stessa_sub_category == 'Rents') or
                            (stessa_category == 'Transfers' and stessa_sub_category == 'Owner Distributions')):
                            rent_date = parse_date(rent_tx.transaction_date)
                            if rent_date:
                                date_diff = abs((s_date - rent_date).days)
                                distribution_candidates.append((s_tx, date_diff))
            
            # Match the best candidate (closest date)
            if distribution_candidates:
                distribution_candidates.sort(key=lambda x: x[1])
                best_s_tx, best_diff = distribution_candidates[0]
                
                # Create match record linking to the rent transaction (as primary)
                match = ReconciliationMatch(
                    stessa_id=best_s_tx.id,
                    renshaw_id=rent_tx.id,  # Link to rent transaction
                    match_score=0.90,
                    match_type='renshaw_distribution',
                    notes=f"Renshaw owner distribution: {expected_distribution:.2f} (Rent ${rent_tx.amount:.2f} - Mgmt Fee ${abs(mgmt_tx.amount):.2f}), Property: {lone_rock_prop.stessa_name}, Date diff={best_diff}d"
                )
                session.add(match)
                matched_stessa_ids.add(best_s_tx.id)
                matches_count += 1
    
    # --- PHASE 6: Allstar Transaction Matching ---
    print("PHASE 6: Matching Allstar transactions with Stessa...")
    matched_allstar_ids = set()
    
    # Get the Malacca St property
    malacca_prop = session.query(Property).filter(Property.stessa_name.ilike('%malacca%')).first()
    
    for allstar_tx in allstar_txs:
        if allstar_tx.id in matched_allstar_ids:
            continue
        
        allstar_date = parse_date(allstar_tx.transaction_date)
        if not allstar_date:
            continue
        
        allstar_amount = allstar_tx.amount
        allstar_category = (allstar_tx.stessa_category or '').strip()
        allstar_sub_category = (allstar_tx.stessa_sub_category or '').strip()
        
        # Must match property
        if not allstar_tx.property_id or not malacca_prop:
            continue
        
        if allstar_tx.property_id != malacca_prop.id:
            continue
        
        # First, try exact single transaction match
        single_match = None
        for s_tx in stessa_txs:
            if s_tx.id in matched_stessa_ids:
                continue
            
            if s_tx.property_id != malacca_prop.id:
                continue
            
            stessa_category = (s_tx.category or '').strip()
            stessa_sub_category = (s_tx.sub_category or '').strip()
            
            if stessa_category.lower() != allstar_category.lower():
                continue
            
            # For Management Fees, use standardized sub-category matching
            if allstar_category.lower() == 'management fees':
                if not matches_management_fee_subcategory(stessa_sub_category, allstar_sub_category):
                    continue
            # For other categories: if either is empty, consider it a match
            # Also allow if one contains the other (e.g., "Gas" vs "Gas & Electric")
            elif allstar_sub_category and stessa_sub_category:
                if (stessa_sub_category.lower() != allstar_sub_category.lower() and
                    allstar_sub_category.lower() not in stessa_sub_category.lower() and
                    stessa_sub_category.lower() not in allstar_sub_category.lower()):
                    continue
            
            s_date = parse_date(s_tx.date)
            if not s_date:
                continue
            
            # Amount match - handle sign differences for utilities
            if allstar_category.lower() == "utilities":
                if allstar_sub_category.lower() == "water & sewer":
                    # Allstar shows as income (positive), Stessa as expense (negative)
                    # Compare absolute values
                    amount_match = abs(abs(s_tx.amount) - abs(allstar_amount)) < 0.01
                    # Also check transaction type matches sign
                    if allstar_tx.transaction_type == "Income" and s_tx.amount < 0:
                        amount_match = False
                    if allstar_tx.transaction_type == "Expense" and s_tx.amount > 0:
                        amount_match = False
                elif allstar_sub_category.lower() == "gas":
                    # Gas should match by exact amount and same sign
                    amount_match = abs(s_tx.amount - allstar_amount) < 0.01
                else:
                    amount_match = abs(s_tx.amount - allstar_amount) < 0.01
            else:
                amount_match = abs(s_tx.amount - allstar_amount) < 0.01
            
            if amount_match:
                date_diff = abs((s_date - allstar_date).days)
                # For utilities, prefer transactions on or after the statement date
                # (utility bills are typically paid after the statement date)
                if allstar_category.lower() == "utilities":
                    if s_date < allstar_date:
                        # Transaction before statement date - less likely to be correct
                        # Only consider if it's very close (within 5 days)
                        if date_diff > 5:
                            continue
                    # Calculate date difference (positive if after statement date)
                    date_diff_after = (s_date - allstar_date).days
                    # Prefer transactions after the statement date
                    if date_diff <= 30:
                        if single_match is None:
                            single_match = (s_tx, date_diff, date_diff_after)
                        else:
                            _, best_diff, best_diff_after = single_match
                            # Prefer transaction after statement date, or closer date if both before/after
                            if date_diff_after >= 0 and best_diff_after < 0:
                                single_match = (s_tx, date_diff, date_diff_after)
                            elif date_diff_after >= 0 and best_diff_after >= 0:
                                if date_diff < best_diff:
                                    single_match = (s_tx, date_diff, date_diff_after)
                            elif date_diff_after < 0 and best_diff_after < 0:
                                if date_diff < best_diff:
                                    single_match = (s_tx, date_diff, date_diff_after)
                else:
                    if date_diff <= 30:
                        if single_match is None or date_diff < single_match[1]:
                            single_match = (s_tx, date_diff, 0)  # Add dummy third element for consistency
        
        # If exact match found, use it
        if single_match:
            best_s_tx, best_diff, _ = single_match
            match = ReconciliationMatch(
                stessa_id=best_s_tx.id,
                allstar_id=allstar_tx.id,
                match_score=1.0,
                match_type='allstar',
                notes=f"Allstar match: {allstar_tx.account_name} ({allstar_tx.transaction_type}), Property: {malacca_prop.stessa_name}, Date diff={best_diff}d, Amount={allstar_amount:.2f}"
            )
            session.add(match)
            matched_stessa_ids.add(best_s_tx.id)
            matched_allstar_ids.add(allstar_tx.id)
            matches_count += 1
            continue
        
        # Try split payment matching
        candidate_txs = []
        for s_tx in stessa_txs:
            if s_tx.id in matched_stessa_ids:
                continue
            
            if s_tx.property_id != malacca_prop.id:
                continue
            
            stessa_category = (s_tx.category or '').strip()
            stessa_sub_category = (s_tx.sub_category or '').strip()
            
            if stessa_category.lower() != allstar_category.lower():
                continue
            
            # For Management Fees, use standardized sub-category matching
            if allstar_category.lower() == 'management fees':
                if not matches_management_fee_subcategory(stessa_sub_category, allstar_sub_category):
                    continue
            # For other categories: sub-category matching with flexibility
            elif allstar_sub_category and stessa_sub_category:
                if (stessa_sub_category.lower() != allstar_sub_category.lower() and
                    allstar_sub_category.lower() not in stessa_sub_category.lower() and
                    stessa_sub_category.lower() not in allstar_sub_category.lower()):
                    continue
            
            s_date = parse_date(s_tx.date)
            if not s_date:
                continue
            
            date_diff = abs((s_date - allstar_date).days)
            if date_diff <= 30:
                # For Utilities/Water & Sewer, handle sign differences
                if allstar_category.lower() == "utilities" and allstar_sub_category.lower() == "water & sewer":
                    candidate_txs.append((s_tx, date_diff))
                elif allstar_category.lower() == "utilities" and allstar_sub_category.lower() == "gas":
                    # Gas should match by exact amount and same sign
                    if abs(s_tx.amount - allstar_amount) < 0.01:
                        candidate_txs.append((s_tx, date_diff))
                elif (s_tx.amount * allstar_amount) > 0:  # Same sign
                    candidate_txs.append((s_tx, date_diff))
        
        # Try combinations of 2-5 transactions
        if candidate_txs:
            candidate_txs.sort(key=lambda x: x[1])
            
            for combo_size in range(2, min(6, len(candidate_txs) + 1)):
                for combo in combinations(candidate_txs, combo_size):
                    combo_txs = [tx for tx, _ in combo]
                    if any(tx.id in matched_stessa_ids for tx in combo_txs):
                        continue
                    
                    total_amount = sum(tx.amount for tx in combo_txs)
                    # For Utilities/Water & Sewer, compare absolute values
                    if allstar_category.lower() == "utilities" and allstar_sub_category.lower() == "water & sewer":
                        amount_match = abs(abs(total_amount) - abs(allstar_amount)) < 0.01
                    else:
                        amount_match = abs(total_amount - allstar_amount) < 0.01
                    
                    if amount_match:
                        primary_tx, primary_diff = combo[0]
                        # Create primary match with full details
                        match = ReconciliationMatch(
                            stessa_id=primary_tx.id,
                            allstar_id=allstar_tx.id,
                            match_score=0.95,
                            match_type='allstar_split',
                            notes=f"Allstar split payment match: {allstar_tx.account_name} ({allstar_tx.transaction_type}), {len(combo_txs)} transactions totaling ${allstar_amount:.2f}, Property: {malacca_prop.stessa_name}, Date diff={primary_diff}d"
                        )
                        session.add(match)
                        matched_stessa_ids.add(primary_tx.id)
                        matches_count += 1
                        
                        # Create match records for remaining transactions (link them to the same Allstar transaction)
                        for tx, _ in combo[1:]:
                            match = ReconciliationMatch(
                                stessa_id=tx.id,
                                allstar_id=allstar_tx.id,
                                match_score=0.95,
                                match_type='allstar_split',
                                notes=f"Allstar split payment (part of {len(combo_txs)} transactions totaling ${allstar_amount:.2f})"
                            )
                            session.add(match)
                            matched_stessa_ids.add(tx.id)
                            matches_count += 1
                        
                        matched_allstar_ids.add(allstar_tx.id)
                        break
                
                if allstar_tx.id in matched_allstar_ids:
                    break
    
    # --- PHASE 7: Mike & Mikes Transaction Matching ---
    print("PHASE 7: Matching Mike & Mikes transactions with Stessa...")
    matched_mike_mikes_ids = set()
    
    # Get the 4708 N 36th St property
    mike_mikes_prop = session.query(Property).filter(
        Property.stessa_name.ilike('%36th%') | 
        Property.address_display.ilike('%36th%')
    ).first()
    
    for mike_mikes_tx in mike_mikes_txs:
        if mike_mikes_tx.id in matched_mike_mikes_ids:
            continue
        
        mm_date = parse_date(mike_mikes_tx.transaction_date)
        if not mm_date:
            continue
        
        mm_amount = mike_mikes_tx.amount
        mm_category = (mike_mikes_tx.stessa_category or '').strip()
        mm_sub_category = (mike_mikes_tx.stessa_sub_category or '').strip()
        
        # Must match property
        if not mike_mikes_tx.property_id or not mike_mikes_prop:
            continue
        
        if mike_mikes_tx.property_id != mike_mikes_prop.id:
            continue
        
        # For management fees, prefer monthly aggregation over direct/split matches
        # (since statements show monthly totals that may be split across multiple transactions)
        is_management_fee = mm_category.lower() == 'management fees'
        
        # First, try exact single transaction match
        single_match = None
        for s_tx in stessa_txs:
            if s_tx.id in matched_stessa_ids:
                continue
            
            if s_tx.property_id != mike_mikes_prop.id:
                continue
            
            stessa_category = (s_tx.category or '').strip()
            stessa_sub_category = (s_tx.sub_category or '').strip()
            
            if stessa_category.lower() != mm_category.lower():
                continue
            
            # For Management Fees, use standardized sub-category matching
            if mm_category.lower() == 'management fees':
                if not matches_management_fee_subcategory(stessa_sub_category, mm_sub_category):
                    continue
            # For other categories: if either is empty, consider it a match
            # Also allow if one contains the other
            elif mm_sub_category and stessa_sub_category:
                if (stessa_sub_category.lower() != mm_sub_category.lower() and
                    mm_sub_category.lower() not in stessa_sub_category.lower() and
                    stessa_sub_category.lower() not in mm_sub_category.lower()):
                    continue
            
            s_date = parse_date(s_tx.date)
            if not s_date:
                continue
            
            # Exact amount match
            if abs(s_tx.amount - mm_amount) < 0.01:
                date_diff = abs((s_date - mm_date).days)
                if date_diff <= 30:
                    if single_match is None or date_diff < single_match[1]:
                        single_match = (s_tx, date_diff)
        
        # If exact match found, use it (unless it's a management fee - prefer monthly aggregation for those)
        # For management fees, we want monthly aggregation to handle multiple transactions summing to monthly total
        if single_match and not is_management_fee:
            best_s_tx, best_diff = single_match
            match = ReconciliationMatch(
                stessa_id=best_s_tx.id,
                mike_mikes_id=mike_mikes_tx.id,
                match_score=1.0,
                match_type='mike_mikes',
                notes=f"Mike & Mikes match: {mike_mikes_tx.description} ({mike_mikes_tx.transaction_type}), Property: {mike_mikes_prop.stessa_name}, Date diff={best_diff}d, Amount={mm_amount:.2f}"
            )
            session.add(match)
            matched_stessa_ids.add(best_s_tx.id)
            matched_mike_mikes_ids.add(mike_mikes_tx.id)
            matches_count += 1
            continue
        
        # Try split payment matching (combinations of 2-5 transactions within 30 days)
        # Skip split payment matching for management fees - prefer monthly aggregation
        candidate_txs = []
        if not is_management_fee:
            for s_tx in stessa_txs:
                if s_tx.id in matched_stessa_ids:
                    continue
                
                if s_tx.property_id != mike_mikes_prop.id:
                    continue
                
                stessa_category = (s_tx.category or '').strip()
                stessa_sub_category = (s_tx.sub_category or '').strip()
                
                if stessa_category.lower() != mm_category.lower():
                    continue
                
                # For Management Fees, use standardized sub-category matching
                if mm_category.lower() == 'management fees':
                    if not matches_management_fee_subcategory(stessa_sub_category, mm_sub_category):
                        continue
                # For other categories: sub-category matching with flexibility
                elif mm_sub_category and stessa_sub_category:
                    if (stessa_sub_category.lower() != mm_sub_category.lower() and
                        mm_sub_category.lower() not in stessa_sub_category.lower() and
                        stessa_sub_category.lower() not in mm_sub_category.lower()):
                        continue
                
                s_date = parse_date(s_tx.date)
                if not s_date:
                    continue
                
                date_diff = abs((s_date - mm_date).days)
                if date_diff <= 30:
                    if (s_tx.amount * mm_amount) > 0:  # Same sign
                        candidate_txs.append((s_tx, date_diff))
        
        # Try combinations of 2-5 transactions (only if not management fee)
        if candidate_txs and not is_management_fee:
            candidate_txs.sort(key=lambda x: x[1])
            
            for combo_size in range(2, min(6, len(candidate_txs) + 1)):
                for combo in combinations(candidate_txs, combo_size):
                    combo_txs = [tx for tx, _ in combo]
                    if any(tx.id in matched_stessa_ids for tx in combo_txs):
                        continue
                    
                    total_amount = sum(tx.amount for tx in combo_txs)
                    if abs(total_amount - mm_amount) < 0.01:
                        primary_tx, primary_diff = combo[0]
                        # Create primary match with full details
                        match = ReconciliationMatch(
                            stessa_id=primary_tx.id,
                            mike_mikes_id=mike_mikes_tx.id,
                            match_score=0.95,
                            match_type='mike_mikes_split',
                            notes=f"Mike & Mikes split payment match: {mike_mikes_tx.description} ({mike_mikes_tx.transaction_type}), {len(combo_txs)} transactions totaling ${mm_amount:.2f}, Property: {mike_mikes_prop.stessa_name}, Date diff={primary_diff}d"
                        )
                        session.add(match)
                        matched_stessa_ids.add(primary_tx.id)
                        matches_count += 1
                        
                        # Create match records for remaining transactions (link them to the same Mike & Mikes transaction)
                        for tx, _ in combo[1:]:
                            match = ReconciliationMatch(
                                stessa_id=tx.id,
                                mike_mikes_id=mike_mikes_tx.id,
                                match_score=0.95,
                                match_type='mike_mikes_split',
                                notes=f"Mike & Mikes split payment (part of {len(combo_txs)} transactions totaling ${mm_amount:.2f})"
                            )
                            session.add(match)
                            matched_stessa_ids.add(tx.id)
                            matches_count += 1
                        
                        matched_mike_mikes_ids.add(mike_mikes_tx.id)
                        break
                
                if mike_mikes_tx.id in matched_mike_mikes_ids:
                    break
        
        # If still no match, try monthly aggregation
        # Sum all transactions in the same month that match category/sub-category
        if mike_mikes_tx.id not in matched_mike_mikes_ids:
            # Get all transactions in the same month (same year and month)
            month_candidates = []
            for s_tx in stessa_txs:
                if s_tx.id in matched_stessa_ids:
                    continue
                
                if s_tx.property_id != mike_mikes_prop.id:
                    continue
                
                stessa_category = (s_tx.category or '').strip()
                stessa_sub_category = (s_tx.sub_category or '').strip()
                
                if stessa_category.lower() != mm_category.lower():
                    continue
                
                # For Management Fees, use standardized sub-category matching
                if mm_category.lower() == 'management fees':
                    if not matches_management_fee_subcategory(stessa_sub_category, mm_sub_category):
                        continue
                # For other categories: sub-category matching with flexibility
                elif mm_sub_category and stessa_sub_category:
                    if (stessa_sub_category.lower() != mm_sub_category.lower() and
                        mm_sub_category.lower() not in stessa_sub_category.lower() and
                        stessa_sub_category.lower() not in mm_sub_category.lower()):
                        continue
                
                s_date = parse_date(s_tx.date)
                if not s_date:
                    continue
                
                # Check if same month and year
                if s_date.year == mm_date.year and s_date.month == mm_date.month:
                    if (s_tx.amount * mm_amount) > 0:  # Same sign
                        month_candidates.append(s_tx)
            
            # Sum all month candidates and check if total matches
            if month_candidates:
                # Filter out already matched transactions for the sum calculation
                unmatched_candidates = [tx for tx in month_candidates if tx.id not in matched_stessa_ids]
                if unmatched_candidates:
                    total_month_amount = sum(tx.amount for tx in unmatched_candidates)
                    if abs(total_month_amount - mm_amount) < 0.01:
                        # Found a monthly match! Create match records for all unmatched transactions
                        primary_tx = unmatched_candidates[0]
                        # Create primary match with full details
                        match = ReconciliationMatch(
                            stessa_id=primary_tx.id,
                            mike_mikes_id=mike_mikes_tx.id,
                            match_score=0.90,
                            match_type='mike_mikes_monthly',
                            notes=f"Mike & Mikes monthly aggregation match: {mike_mikes_tx.description} ({mike_mikes_tx.transaction_type}), {len(unmatched_candidates)} transactions in {mm_date.strftime('%B %Y')} totaling ${mm_amount:.2f}, Property: {mike_mikes_prop.stessa_name}"
                        )
                        session.add(match)
                        matched_stessa_ids.add(primary_tx.id)
                        matches_count += 1
                        
                        # Create match records for remaining transactions (link them to the same Mike & Mikes transaction)
                        for tx in unmatched_candidates[1:]:
                            match = ReconciliationMatch(
                                stessa_id=tx.id,
                                mike_mikes_id=mike_mikes_tx.id,
                                match_score=0.90,
                                match_type='mike_mikes_monthly',
                                notes=f"Mike & Mikes monthly aggregation (part of {len(unmatched_candidates)} transactions totaling ${mm_amount:.2f})"
                            )
                            session.add(match)
                            matched_stessa_ids.add(tx.id)
                            matches_count += 1
                        
                        matched_mike_mikes_ids.add(mike_mikes_tx.id)
    
    session.commit()
    print(f"Reconciliation finished. Total matches: {matches_count}")
    if unsplit_mortgages:
        print(f"Found {len(unsplit_mortgages)} unsplit mortgage payment(s) needing to be split.")
    generate_report(session, unsplit_mortgages, year)
    session.close()

def is_no_reconciliation_source(category, sub_category):
    """
    Determine if a transaction category/sub-category has no reconciliation source available.
    These are transactions like Insurance, Taxes, HOA dues that won't be matched against
    property management systems or mortgage statements.
    """
    cat_lower = (category or '').strip().lower()
    sub_cat_lower = (sub_category or '').strip().lower()
    
    # Insurance transactions
    if cat_lower == 'insurance':
        return True
    
    # Tax transactions
    if cat_lower == 'taxes':
        return True
    
    # HOA dues and related admin items
    if cat_lower == 'admin & other':
        if 'hoa' in sub_cat_lower or 'dues' in sub_cat_lower:
            return True
        if 'licenses' in sub_cat_lower:
            return True
        if 'bank fees' in sub_cat_lower:
            return True
    
    return False

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
        # For mortgage statements, count by payment_due_date (with fallback to statement_date)
        mortgage_year_count = len([m for m in all_mortgage if (
            (parse_date(m.payment_due_date) and parse_date(m.payment_due_date).year == year) or
            (not m.payment_due_date and parse_date(m.statement_date) and parse_date(m.statement_date).year == year)
        )])
        
        all_costar = session.query(CostarRaw).all()
        costar_year_count = len(filter_by_year(all_costar, 'completed_on', year))
    
    # Calculate unmatched counts for filtered year
    # Include all matches (automatic and manual)
    matched_stessa_ids_all = [m.stessa_id for m in session.query(ReconciliationMatch).filter(
        ReconciliationMatch.stessa_id.isnot(None)
    ).all()]
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
    unmatched_stessa_all = session.query(StessaRaw).filter(
        ~StessaRaw.id.in_(matched_stessa_ids),
        StessaRaw.is_filtered == False
    ).all()
    
    # Apply year filter if specified
    if year:
        unmatched_stessa_all = filter_by_year(unmatched_stessa_all, 'date', year)
    
    # Separate transactions with no reconciliation source from other unmatched transactions
    no_recon_source = []
    unmatched_stessa = []
    
    for tx in unmatched_stessa_all:
        if is_no_reconciliation_source(tx.category, tx.sub_category):
            no_recon_source.append(tx)
        else:
            unmatched_stessa.append(tx)
    
    # Report transactions with no reconciliation source available
    if no_recon_source:
        # Sort by property (ascending), then category (ascending), then date (ascending)
        no_recon_source.sort(key=lambda x: (
            (x.property or '').strip().lower(),
            (x.category or '').strip().lower(),
            parse_date(x.date) or datetime.date.min
        ))
        
        print(f"\nNO RECONCILIATION SOURCE AVAILABLE ({len(no_recon_source)}):")
        print("ℹ️  These transactions (Insurance, Taxes, HOA dues, etc.) don't have reconciliation sources.")
        print("    They are expected to remain unmatched and are excluded from the unmatched Stessa report.")
        print(f"  {'Date':12} | {'Amount':>10} | {'Payee':30} | {'Category':25} | {'Property':25}")
        print(f"  {'-'*12} | {'-'*10} | {'-'*30} | {'-'*25} | {'-'*25}")
        
        for tx in no_recon_source:
            tx_date = parse_date(tx.date)
            date_str = tx_date.strftime('%m/%d/%Y') if tx_date else tx.date
            prop_display = (tx.property or 'N/A')[:25]
            payee_display = (tx.name or 'N/A')[:30]
            category_display = f"{tx.category or 'N/A'}/{tx.sub_category or ''}"[:25]
            print(f"  {date_str:12} | ${tx.amount:>9.2f} | {payee_display:30} | {category_display:25} | {prop_display:25}")
    
    print(f"\nUNMATCHED STESSA:")
    # Sort by property (ascending), then category (ascending), then date (ascending)
    unmatched_stessa.sort(key=lambda x: (
        (x.property or '').strip().lower(),
        (x.category or '').strip().lower(),
        parse_date(x.date) or datetime.date.min
    ))
    
    # Print header with aligned columns
    print(f"  {'Date':12} | {'Amount':>10} | {'Payee':25} | {'Category':20} | {'Property':20}")
    print(f"  {'-'*12}-|-{'-'*10}-|-{'-'*25}-|-{'-'*20}-|-{'-'*20}")
    
    for tx in unmatched_stessa:
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
    # Exclude transactions with no reconciliation source
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
            if item.get('amount_diff', 0) > 0.01:
                print(f"    → Amount diff: ${item['amount_diff']:.2f} (needs correction in Stessa)")
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

def interactive_reconciliation_mode(year=None):
    """
    Interactive mode for manually marking transactions as reconciled.
    Shows unmatched Stessa transactions and allows user to:
    - Mark as reconciled with a reason
    - Skip (leave unreconciled)
    - View transaction details
    - Exit and save progress
    """
    engine, Session = init_db()
    session = Session()
    
    print("=" * 80)
    print("INTERACTIVE RECONCILIATION MODE")
    print("=" * 80)
    print("\nThis mode allows you to manually mark transactions as reconciled.")
    print("Transactions marked here will be excluded from unmatched reports.\n")
    
    # Get unmatched Stessa transactions
    matched_ids = set([m.stessa_id for m in session.query(ReconciliationMatch).filter(
        ReconciliationMatch.stessa_id.isnot(None)
    ).all()])
    
    # Get all unmatched, unfiltered Stessa transactions
    unmatched = session.query(StessaRaw).filter(
        ~StessaRaw.id.in_(matched_ids),
        StessaRaw.is_filtered == False
    ).all()
    
    # Filter by year if specified
    if year:
        unmatched = [tx for tx in unmatched if parse_date(tx.date) and parse_date(tx.date).year == year]
        print(f"Filtering to year {year}...")
    
    if not unmatched:
        print("No unmatched transactions found.")
        session.close()
        return
    
    print(f"Found {len(unmatched)} unmatched transactions.\n")
    
    # Group by property for easier navigation
    from collections import defaultdict
    by_property = defaultdict(list)
    for tx in unmatched:
        if tx.property_id:
            prop = session.query(Property).filter(Property.id == tx.property_id).first()
            prop_name = prop.stessa_name if prop else "Unknown Property"
        else:
            prop_name = "No Property"
        by_property[prop_name].append(tx)
    
    # Sort transactions by property, then date
    for prop_name in sorted(by_property.keys()):
        by_property[prop_name].sort(key=lambda x: parse_date(x.date) or datetime.date(1900, 1, 1))
    
    # Flatten back to list with numbering
    numbered_txs = []
    idx = 1
    for prop_name in sorted(by_property.keys()):
        for tx in by_property[prop_name]:
            numbered_txs.append((idx, tx, prop_name))
            idx += 1
    
    # Main loop
    start_idx = 0
    batch_size = 20
    
    while True:
        # Display batch of transactions
        end_idx = min(start_idx + batch_size, len(numbered_txs))
        current_batch = numbered_txs[start_idx:end_idx]
        
        if not current_batch:
            print("\nAll transactions processed!")
            session.commit()
            session.close()
            return
        
        print("\n" + "=" * 80)
        print(f"Showing transactions {start_idx + 1}-{end_idx} of {len(numbered_txs)}")
        print("=" * 80)
        print(f"{'#':<5} {'Date':<12} {'Amount':<12} {'Category':<25} {'Property':<30}")
        print("-" * 80)
        
        for num, tx, prop_name in current_batch:
            date_str = tx.date[:10] if len(tx.date) >= 10 else tx.date
            amount_str = f"${tx.amount:,.2f}"
            category_str = f"{tx.category}/{tx.sub_category}"[:24]
            prop_str = prop_name[:29]
            print(f"{num:<5} {date_str:<12} {amount_str:<12} {category_str:<25} {prop_str:<30}")
        
        print("\nCommands:")
        print("  <number>     - Mark transaction as reconciled (will prompt for reason)")
        print("  d<number>    - Show details for transaction")
        print("  s<number>    - Skip transaction (leave unreconciled)")
        print("  n            - Next batch")
        print("  p            - Previous batch")
        print("  q            - Quit and save progress")
        
        user_input = input("\nEnter command: ").strip().lower()
        
        if user_input == 'q':
            session.commit()
            print("\nProgress saved. Exiting interactive mode.")
            session.close()
            return
        
        if user_input == 'n':
            start_idx = min(start_idx + batch_size, len(numbered_txs))
            continue
        
        if user_input == 'p':
            start_idx = max(0, start_idx - batch_size)
            continue
        
        if user_input.startswith('d'):
            # Show details
            try:
                num = int(user_input[1:])
                # Find transaction by number
                found = None
                for n, tx, prop in numbered_txs:
                    if n == num:
                        found = (tx, prop)
                        break
                
                if found:
                    tx, prop_name = found
                    print("\n" + "=" * 80)
                    print("TRANSACTION DETAILS")
                    print("=" * 80)
                    print(f"Number: {num}")
                    print(f"Date: {tx.date}")
                    print(f"Amount: ${tx.amount:,.2f}")
                    print(f"Name: {tx.name}")
                    print(f"Category: {tx.category}")
                    print(f"Sub-Category: {tx.sub_category}")
                    print(f"Property: {prop_name}")
                    print(f"Notes: {tx.notes or 'N/A'}")
                    print(f"Details: {tx.details or 'N/A'}")
                    print("=" * 80)
                    input("\nPress Enter to continue...")
                else:
                    print(f"Transaction #{num} not found in current batch.")
            except ValueError:
                print("Invalid format. Use 'd<number>' (e.g., 'd5')")
            continue
        
        if user_input.startswith('s'):
            # Skip transaction
            try:
                num = int(user_input[1:])
                print(f"Transaction #{num} skipped (left unreconciled).")
                # Just continue - don't remove from list so user can come back to it
            except ValueError:
                print("Invalid format. Use 's<number>' (e.g., 's5')")
            continue
        
        # Regular number - mark as reconciled
        try:
            num = int(user_input)
            # Find transaction by number
            found = None
            for n, tx, prop in numbered_txs:
                if n == num:
                    found = (tx, prop)
                    break
            
            if found:
                tx, prop_name = found
                
                # Show transaction details
                print("\n" + "=" * 80)
                print("MARKING TRANSACTION AS RECONCILED")
                print("=" * 80)
                print(f"Date: {tx.date}")
                print(f"Amount: ${tx.amount:,.2f}")
                print(f"Name: {tx.name}")
                print(f"Category: {tx.category}/{tx.sub_category}")
                print(f"Property: {prop_name}")
                print("=" * 80)
                
                # Get reconciliation reason
                print("\nWhy is this transaction reconciled?")
                print("(e.g., 'One-time expense', 'No reconciliation source', 'Verified manually', 'Insurance payment')")
                reason = input("Reason: ").strip()
                
                if not reason:
                    print("No reason provided. Transaction not marked as reconciled.")
                    continue
                
                # Create match record
                match = ReconciliationMatch(
                    stessa_id=tx.id,
                    match_score=1.0,
                    match_type='manual_reconciled',
                    notes=f"Manually reconciled: {reason}"
                )
                session.add(match)
                session.commit()
                
                print(f"\n✓ Transaction #{num} marked as reconciled: {reason}")
                
                # Remove from list
                numbered_txs = [(n, t, p) for n, t, p in numbered_txs if n != num]
                # Renumber remaining transactions
                numbered_txs = [(i+1, tx, prop) for i, (_, tx, prop) in enumerate(numbered_txs)]
                
                # Adjust start_idx if needed
                if start_idx >= len(numbered_txs):
                    start_idx = max(0, len(numbered_txs) - batch_size)
            else:
                print(f"Transaction #{num} not found in current batch.")
        except ValueError:
            print("Invalid input. Enter a number, 'd<number>', 's<number>', 'n', 'p', or 'q'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Reconcile mortgage, Stessa, and Property Boss transactions')
    parser.add_argument('--year', type=int, default=2025, 
                        help='Calendar year to reconcile (default: 2025). Use --year 0 to reconcile all years.')
    parser.add_argument('--interactive', action='store_true',
                        help='Enter interactive mode for manually marking transactions as reconciled')
    parser.add_argument('--clear-manual', action='store_true',
                        help='Clear manually reconciled transactions before running reconciliation')
    args = parser.parse_args()
    
    # Convert 0 to None for "all years"
    year = args.year if args.year != 0 else None
    
    if args.interactive:
        interactive_reconciliation_mode(year=year)
    else:
        run_reconciliation(year=year, clear_manual=args.clear_manual)
