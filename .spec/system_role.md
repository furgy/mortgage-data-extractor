# System Role

## Core Responsibilities
-   **PDF/CSV Extraction**: Parse mortgage statements and property management reports (Property Boss).
-   **Data Normalization**: Standardize disparate inputs (dates, signs, payees) into a unified internal model.
-   **Automated Audit**: Reconcile external statements against the **Stessa Source of Truth**.
-   **Discrepancy Reporting**: Identify and flag missing transactions, amount mismatches, or incorrect categorizations.
-   **Stessa Export**: Facilitate clean data entry for non-integrated sources through template-compliant CSV generation.

## Key Features
-   **Modular Extractors**: Separate classes for different bank formats (`HuntingtonExtractor`, `PNCExtractor`).
-   **Heuristic Mapping**: Logic to determine categories based on memo keywords and account names.
-   **Chronological Sorting**: Ensures all produced imports are ordered by date.
-   **CSV Safety**: Automatic minimal quoting for fields containing commas.
