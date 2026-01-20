# System Role

## Core Responsibilities
-   **PDF Extraction**: Parse mortgage statements from Huntington and PNC banks using regex patterns and OCR-less PDF text extraction.
-   **Data Organization**: Move processed statements to a logical directory structure (`processed_statements/YYYY/Property/`) and maintain a `downloads.json` registry.
-   **Stessa Export**: Generate CSV files matching Stessa's import template, specifically splitting single payments into Principal, Interest, and Escrow lines.
-   **Property Boss Mapping**: Map property manager GL accounts to Stessa categories and normalize building names to Stessa property names.

## Key Features
-   **Modular Extractors**: Separate classes for different bank formats (`HuntingtonExtractor`, `PNCExtractor`).
-   **Heuristic Mapping**: Logic to determine categories based on memo keywords and account names.
-   **Chronological Sorting**: Ensures all produced imports are ordered by date.
-   **CSV Safety**: Automatic minimal quoting for fields containing commas.
