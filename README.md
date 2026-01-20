# Mortgage Data Extractor & Stessa Mapper

An automated utility for extracting mortgage transaction data from bank PDF statements and mapping property management records to Stessa sub-categories.

## 🚀 Key Features
- **PDF Extraction**: Automated parsing of Huntington and PNC mortgage statements.
- **Transaction Splitting**: Automatically handles Principal, Interest, and Escrow breakdowns.
- **Smart Mapping**: Heuristic-based categorization of Property Boss transaction logs.
- **Stessa Ready**: Generates import-compliant CSV files with correct sign conventions and property naming.

## 📖 Documentation (OpenSpec)
This project follows the [OpenSpec](https://openspec.dev) standard. Detailed documentation can be found in the `.spec/` directory:

- [Product Context](.spec/product_context.md): Why this project exists and what it solves.
- [System Role](.spec/system_role.md): Technical responsibilities and core features.
- [Tech Stack](.spec/tech_stack.md): Languages, libraries, and tools used.
- [Active Context](.spec/active_context.md): Current development status and focus.
- [Decisions](.spec/decisions.md): Documentation of key architectural choices.
- [Progress](.spec/progress.md): Historical milestone tracking.
- [User Manual](.spec/user_manual.md): How to install and run the workflows.

## 🛠 Usage
For detailed instructions on how to use the different extraction and mapping workflows, please refer to the [User Manual](.spec/user_manual.md).

---
*Maintained by Shawn (furgy)*
