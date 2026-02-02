### *Structure of the repo*

```text
├── filing_data_structure.md         # Documentation for filing data
├── heuristic_process/               # Heuristic extract tools and filing processing (LLMs applied processes are here)
├── manager/                         # High-level entity and sector management
│   ├── entity_manager.py            # Initiating tables for entity level
│   ├── sector_manager.py            # Initiating tables for sector level
│   ├── adding/                      # Adding deltas for facets
│   └── initial/                     # Initiating facets with anchor snapshots
├── schema/                          # Data models (Pydantic/JSON schemas)
├── utils/                           # Deterministic utilities and extract tools
├── SnP500_filings/                  # Data folder which not appear on github, see filing_data_structure.md for further information
├── csv_statement/                   # Data folder stores statements captured from filing 10-K and 10-Q of companies. There are 6 type of statement: income_statement.csv, balancesheet.csv, cash_flow.csv, comprehensive_income.csv, equity_statement.csv, schedule_of_investment.csv (4, 5, and 6 are optional)
├── temp/                            # Temporary code/file, not appear on github
├── pyvenv.cfg
└── requirements.txt
```