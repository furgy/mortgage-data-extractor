# Architectural Decisions

## 1. Modular Extractor Architecture
-   **Decision**: Use a base `Extractor` class and child classes for each bank.
-   **Rationale**: Bank statements change formats frequently. This allows adding or updating a single bank without affecting the others.

## 2. Description Formatting
-   **Decision**: Use a vertical pipe (` | `) and an `((PBI))` tag.
-   **Rationale**: Physical newlines cause CSV parsers to treat one record as two. A pipe keeps the data on one line while remaining human-readable in Stessa.

## 3. Amount Inversion
-   **Decision**: Invert amounts during the final transformation step.
-   **Rationale**: Property Boss and Stessa have opposite sign conventions for Income/Expense. Inverting ensures Stessa reports reflect the correct cash flow direction.

## 5. Stessa as Source of Truth
-   **Decision**: Treat Stessa as the master database for reconciliation.
-   **Rationale**: The owner prefers a "Fix in Stessa, re-run" workflow to ensure the core platform remains accurate and trusted.

## 6. Transaction Exclusion (Filtering)
-   **Decision**: Filter out specific GL accounts (e.g., Security Deposits) from the Property Boss import logic.
-   **Rationale**: Certain liabilities or non-income/expense items handled by the manager are not carried on the owner's operational books.

## 7. Database Selection (SQLite)
-   **Decision**: Use SQLite for local data persistence.
-   **Rationale**: SQLite is serverless, zero-config, and stores everything in a single file. This allows for complex reconciliation queries (SQL) without the overhead of a managed database.

## 8. Raw-to-Truth Architecture
-   **Decision**: Store external records in "Raw" tables mirroring source formats, while the Stessa table acts as the "Truth" anchor.
-   **Rationale**: This preserves data provenance (auditability) and allows transformation logic (sign inversion, filtering) to reside in the data layer.

## 9. Git Ignore Strategy
-   **Decision**: Ignore all PDFs and local CSV data files.
-   **Rationale**: These files contain sensitive financial information and property addresses that should not be stored in version control. Templates and code remain tracked.
