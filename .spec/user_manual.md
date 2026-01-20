# User Manual

## Installation
1.  Clone the repository.
2.  Install dependencies: `uv pip install -r requirements.txt`.

## Workflow 1: Mortgage Statements
1.  Place new PDF statements in the `statements/` directory.
2.  Run the processor: `python main.py`.
3.  Generate the Stessa import file: `python stessa_exporter.py`.
4.  Upload `stessa_import.csv` to Stessa.

## Workflow 2: Property Boss Mapping
1.  Export your Property Boss transaction list as `Property_Boss_Transactions-2025.csv`.
2.  Run the mapper: `python map_pb_to_stessa.py`.
3.  Generate the final import: `python transform_pb_to_stessa_import.py`.
4.  Upload `pb_stessa_import.csv` to Stessa.

## Maintenance
-   To update mapping categories, edit the `map_transaction` function in `map_pb_to_stessa.py`.
-   To update property names, edit the `PROPERTY_MAP` dict in `transform_pb_to_stessa_import.py`.
