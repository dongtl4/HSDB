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
├── temp/                            # Temporary code/file, not appear on github
├── pyvenv.cfg
└── requirements.txt
```