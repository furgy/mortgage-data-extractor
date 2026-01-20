# Progress Tracker

## Completed Phases
-   [x] Initial Huntington PDF extraction logic.
-   [x] Batch processing and `downloads.json` registry setup.
-   [x] Support for PNC mortgage statements.
-   [x] Stessa CSV Exporter with principal/interest/escrow splitting.
-   [x] Property Boss GL Account mapping heuristics.
-   [x] Transformation script for Stessa import template compliance.
-   [x] Git repository initialization and remote push.
-   [x] OpenSpec documentation implementation.

## In Progress
-   [ ] **Phase 2: Reconciliation Engine Architecture**
    -   [ ] Create Python data models for `StessaTransaction` and `StatementTransaction`.
    -   [ ] Implement fuzzy matching logic (Date/Amount/Payee).
    -   [ ] Build automated audit report (PDF/Markdown).

## Future Plans
-   [ ] Automated PDF downloading via browser automation.
-   [ ] Support for additional mortgage providers.
-   [ ] GUI or interface for manual mapping adjustments.
