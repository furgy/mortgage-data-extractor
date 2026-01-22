from sqlalchemy import Column, Integer, String, Float, Date, Boolean, create_engine, ForeignKey, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Property(Base):
    """
    Master property list loaded from properties.csv
    """
    __tablename__ = 'properties'
    
    id = Column(Integer, primary_key=True)
    stessa_name = Column(String, unique=True)
    mortgage_loan_number = Column(String)
    # Address Components for linking Property Boss
    street = Column(String)
    city = Column(String)
    state = Column(String)
    zip_code = Column(String)
    
    # Display label
    address_display = Column(String)
    
    # Management flags
    is_pb_managed = Column(Boolean, default=True)  # True if managed in Property Boss, False otherwise

class StessaRaw(Base):
    """
    Mirrors the exact format of the Stessa CSV export (stessa_import_format.csv).
    """
    __tablename__ = 'stessa_raw'
    
    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey('properties.id')) # Link to master property
    date = Column(String)  # Stored as string to match CSV, can be parsed to Date in views/queries
    name = Column(String)
    notes = Column(String)
    details = Column(String)
    category = Column(String)
    sub_category = Column(String)
    amount = Column(Float)
    portfolio = Column(String)
    property = Column(String)
    unit = Column(String)
    data_source = Column(String)
    account = Column(String)
    owner = Column(String)
    attachments = Column(String)
    is_filtered = Column(Boolean, default=False)
    filter_reason = Column(String)

class PropertyBossRaw(Base):
    """
    Mirrors the exact columns from the Property Boss transaction CSV.
    """
    __tablename__ = 'property_boss_raw'
    
    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey('properties.id')) # Link to master property
    buildingName = Column(String)
    unitNumber = Column(String)
    entryDate = Column(String)
    glAccountId = Column(String)
    glAccountName = Column(String)
    glAccountTypeId = Column(String)
    glAccountSubTypeId = Column(String)
    glAccountExcludedFromCashBalances = Column(String)
    parentGLAccountName = Column(String)
    combinedGLAccountName = Column(String)
    payeeName = Column(String)
    postingMemo = Column(String)
    buildingReserve = Column(String)
    amount = Column(Float)
    currentLiabilities = Column(String)
    additionsToCash = Column(String)
    subtractionsFromCash = Column(String)
    accountType = Column(String)
    accountTypeOrderId = Column(String)
    ownerName = Column(String)
    journalId = Column(String)
    journalCodeId = Column(String)
    attributeId = Column(String)
    buildingType = Column(String)
    countryId = Column(String)
    addressLine1 = Column(String)
    addressLine2 = Column(String)
    addressLine3 = Column(String)
    city = Column(String)
    state = Column(String)
    zipCode = Column(String)
    buildingId = Column(String)
    unitId = Column(String)
    rentalOwnerId = Column(String)
    buildingStatusId = Column(String)
    showTenantLiabilities = Column(String)
    unpaidBillAmount = Column(String)
    pendingEpayAmount = Column(String)
    is_filtered = Column(Boolean, default=False)
    filter_reason = Column(String)

class ReconciliationMatch(Base):
    __tablename__ = 'reconciliation_matches'
    
    id = Column(Integer, primary_key=True)
    stessa_id = Column(Integer, ForeignKey('stessa_raw.id'))
    pb_id = Column(Integer, ForeignKey('property_boss_raw.id'))
    mortgage_id = Column(Integer, ForeignKey('mortgage_raw.id'))
    costar_id = Column(Integer, ForeignKey('costar_raw.id'))
    realty_medics_id = Column(Integer, ForeignKey('realty_medics_raw.id'))
    renshaw_id = Column(Integer, ForeignKey('renshaw_raw.id'))
    allstar_id = Column(Integer, ForeignKey('allstar_raw.id'))
    mike_mikes_id = Column(Integer, ForeignKey('mike_mikes_raw.id'))
    match_score = Column(Float) # 1.0 for exact, less for fuzzy
    match_type = Column(String) # 'exact', 'fuzzy', 'date_offset', 'mortgage_component', 'costar_rent', 'realty_medics', 'renshaw', 'allstar', 'mike_mikes'
    notes = Column(String)

class CostarRaw(Base):
    """
    Stores rent payment data from Apartments.com (Costar).
    """
    __tablename__ = 'costar_raw'
    
    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey('properties.id')) # Link to master property
    type = Column(String)  # Payment, Monthly Rent Due
    memo = Column(String)
    status = Column(String)  # Completed, Posted
    initiated_on = Column(String)
    completed_on = Column(String)
    credit_amt = Column(Float)
    debit_amt = Column(Float)
    initiated_by = Column(String)
    property_address = Column(String)  # Full address from CSV
    unit = Column(String)
    transaction_id = Column(String)
    reference_id = Column(String)

class MortgageRaw(Base):
    """
    Stores extracted data from bank PDF statements.
    """
    __tablename__ = 'mortgage_raw'
    
    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey('properties.id')) # Link to master property
    bank = Column(String)  # PNC or Huntington
    property_address = Column(String)
    statement_date = Column(String)
    payment_due_date = Column(String)
    amount_due = Column(Float)
    principal_breakdown = Column(Float)
    interest_breakdown = Column(Float)
    escrow_breakdown = Column(Float)
    fees_breakdown = Column(Float)
    outstanding_principal = Column(Float)
    loan_number = Column(String)
    is_valid = Column(Boolean)
    validation_error = Column(String)
    raw_text_record = Column(String)

class RealtyMedicsRaw(Base):
    """
    Stores extracted data from Realty Medics property management CSV reports.
    """
    __tablename__ = 'realty_medics_raw'

    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey('properties.id')) # Link to master property
    account_name = Column(String)
    transaction_type = Column(String) # "Income" or "Expense"
    transaction_date = Column(String) # Date in YYYY-MM-DD format
    month = Column(String) # e.g., "Jan 2025", "Feb 2025"
    amount = Column(Float) # Positive for income, negative for expenses
    stessa_category = Column(String)
    stessa_sub_category = Column(String)

class RenshawRaw(Base):
    """
    Stores extracted data from Renshaw property management HTML reports.
    """
    __tablename__ = 'renshaw_raw'

    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey('properties.id')) # Link to master property
    account_name = Column(String)
    account_code = Column(String)
    transaction_type = Column(String) # "Income" or "Expense"
    transaction_date = Column(String) # Date in YYYY-MM-DD format
    month = Column(String) # e.g., "JAN 25", "FEB 25"
    amount = Column(Float) # Positive for income, negative for expenses
    stessa_category = Column(String)
    stessa_sub_category = Column(String)

class AllstarRaw(Base):
    """
    Stores extracted data from Allstar property management CSV reports.
    """
    __tablename__ = 'allstar_raw'

    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey('properties.id')) # Link to master property
    account_name = Column(String)
    transaction_type = Column(String) # "Income" or "Expense"
    transaction_date = Column(String) # Date in YYYY-MM-DD format
    month = Column(String) # e.g., "Jan 2025", "Feb 2025"
    amount = Column(Float) # Positive for income, negative for expenses
    stessa_category = Column(String)
    stessa_sub_category = Column(String)

class MikeMikesRaw(Base):
    """
    Stores extracted data from Mike & Mikes property management PDF statements.
    """
    __tablename__ = 'mike_mikes_raw'

    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey('properties.id')) # Link to master property
    statement_date = Column(String) # Statement date
    statement_start = Column(String) # Statement period start
    statement_end = Column(String) # Statement period end
    description = Column(String) # Transaction description
    transaction_date = Column(String) # Date in YYYY-MM-DD format
    amount = Column(Float) # Positive for income, negative for expenses
    transaction_type = Column(String) # "Income" or "Expense"
    stessa_category = Column(String)
    stessa_sub_category = Column(String)

def init_db(db_path='reconciliation.db'):
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    
    # Migration: Add is_pb_managed column if it doesn't exist
    with engine.connect() as conn:
        # Check if column exists by querying table info
        result = conn.execute(
            text("SELECT sql FROM sqlite_master WHERE type='table' AND name='properties'")
        )
        table_sql = result.fetchone()
        
        if table_sql and table_sql[0] and 'is_pb_managed' not in table_sql[0]:
            # Column doesn't exist, add it
            print("Migrating database: Adding is_pb_managed column to properties table...")
            conn.execute(
                text("ALTER TABLE properties ADD COLUMN is_pb_managed BOOLEAN DEFAULT 1")
            )
            conn.commit()
            print("Migration complete: is_pb_managed column added (defaulting to True for existing properties)")
        
        # Migration: Add is_filtered and filter_reason columns to stessa_raw if they don't exist
        result = conn.execute(
            text("SELECT sql FROM sqlite_master WHERE type='table' AND name='stessa_raw'")
        )
        table_sql = result.fetchone()
        
        if table_sql and table_sql[0]:
            if 'is_filtered' not in table_sql[0]:
                print("Migrating database: Adding is_filtered and filter_reason columns to stessa_raw table...")
                conn.execute(
                    text("ALTER TABLE stessa_raw ADD COLUMN is_filtered BOOLEAN DEFAULT 0")
                )
                conn.execute(
                    text("ALTER TABLE stessa_raw ADD COLUMN filter_reason VARCHAR")
                )
                conn.commit()
                print("Migration complete: is_filtered and filter_reason columns added to stessa_raw")
        
        # Migration: Add costar_id column to reconciliation_matches if it doesn't exist
        result = conn.execute(
            text("SELECT sql FROM sqlite_master WHERE type='table' AND name='reconciliation_matches'")
        )
        table_sql = result.fetchone()
        
        if table_sql and table_sql[0]:
            if 'costar_id' not in table_sql[0]:
                print("Migrating database: Adding costar_id column to reconciliation_matches table...")
                conn.execute(
                    text("ALTER TABLE reconciliation_matches ADD COLUMN costar_id INTEGER REFERENCES costar_raw(id)")
                )
                conn.commit()
                print("Migration complete: costar_id column added to reconciliation_matches")
            
            if 'realty_medics_id' not in table_sql[0]:
                print("Migrating database: Adding realty_medics_id column to reconciliation_matches table...")
                conn.execute(
                    text("ALTER TABLE reconciliation_matches ADD COLUMN realty_medics_id INTEGER REFERENCES realty_medics_raw(id)")
                )
                conn.commit()
                print("Migration complete: realty_medics_id column added to reconciliation_matches")
            
            if 'renshaw_id' not in table_sql[0]:
                print("Migrating database: Adding renshaw_id column to reconciliation_matches table...")
                conn.execute(
                    text("ALTER TABLE reconciliation_matches ADD COLUMN renshaw_id INTEGER REFERENCES renshaw_raw(id)")
                )
                conn.commit()
                print("Migration complete: renshaw_id column added to reconciliation_matches")
            
            if 'allstar_id' not in table_sql[0]:
                print("Migrating database: Adding allstar_id column to reconciliation_matches table...")
                conn.execute(
                    text("ALTER TABLE reconciliation_matches ADD COLUMN allstar_id INTEGER REFERENCES allstar_raw(id)")
                )
                conn.commit()
                print("Migration complete: allstar_id column added to reconciliation_matches")
            
            if 'mike_mikes_id' not in table_sql[0]:
                print("Migrating database: Adding mike_mikes_id column to reconciliation_matches table...")
                conn.execute(
                    text("ALTER TABLE reconciliation_matches ADD COLUMN mike_mikes_id INTEGER REFERENCES mike_mikes_raw(id)")
                )
                conn.commit()
                print("Migration complete: mike_mikes_id column added to reconciliation_matches")
    
    return engine, sessionmaker(bind=engine)

if __name__ == "__main__":
    init_db()
    print("Database initialized successfully.")
