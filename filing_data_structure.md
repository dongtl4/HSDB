### SEC Filings Data Repository
#### Directory Structure
The data is organized by Ticker Symbol, then by Form Type, and finally by individual filing instances identified by date and accession number.

```text
filings/
└── [TICKER]/                      # e.g., AAPL, TSLA
    └── [FORM_TYPE]/               # e.g., 10-K, 10-Q, Insider_Trading
        └── [YYYY-MM-DD]_[ACCESSION]/ 
            ├── metadata.json      # Structured info about the filing
            ├── [FORM].md or .txt  # The primary document
            └── [ATTACHMENTS]      # Supporting exhibits or graphics
```

This dataset including data of all companies in the **S&P 500** list, acquired from [Wikipedia](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies) on `2026-13-01`.

This dataset acquires total of 6 form type, including 10-K (`10-K`), 10-Q (`10-Q`). 8-K (`8-K`), DEF 14A (`Proxy_Statement`), 4 (`Insider_Trading`), and SC 13D (`Activist_State`). The purpose of these filings are:

* **10-K**: The Annual Report, providing a comprehensive overview of a company's financial performance, audited financial statements, and a detailed description of its business and risks for the entire fiscal year.
* **10-Q**: The Quarterly Report, which includes unaudited financial statements and provides a continuing view of the company's financial position during the first three quarters of the year.
* **8-K**: The Current Report, used to announce major, unscheduled events that shareholders should know about, like management changes, mergers, bankruptcy, or asset sales, generally within four business days of the event.
* **DEF 14A**: The Definitive Proxy Statement, providing shareholders with the information needed to make informed votes at an annual meeting, typically including executive compensation and board member biographies.
* **4**: The Statement of Changes in Beneficial Ownership, used to report "insider" trades by company officers, directors, or any beneficial owners of more than 10% of a class of the company’s equity securities.
* **SC 13D**: The Schedule 13D (often called an Activist Stake filing), required when an entity acquires more than 5% of a company’s stock with the intent to influence or change control of the issuer.

#### Data Components
1. **Metadata `(metadata.json)`**
Every filing folder contains a metadata.json file. This is the "source of truth" for programmatic processing. It includes:

* **Accession Number**: The unique SEC identifier for the filing.
* **Filing Date**: The date the document was officially submitted.
* **Saved Files**: A list of all files in the folder, including their original purpose (e.g., "Primary Document" vs "Exhibits").

2. **Primary Documents**
Markdown (.md): Most filings (10-K, 10-Q, 8-K) are converted to Markdown for easy parsing and LLM compatibility.

TXT: Form 4 (Insider Trading) text version of filings for human reading/LLMs feeding purpose.

3. **Attachments**

* **Exhibits (like Material Contracts or Subsidiary lists)**: additional attachments to the primary document, saved as separate Markdown files. They usually start with `EX-***`.
* **HTML_R(x).md file**: tables shown in the primary document. These files are saved to make table extraction from filings easier. The label of each table is usually stored in the `purpose` attribute in the corresponding `metadata.json` file. For example:

```json
{
    "saved_as": "HTML_R3.md",
    "original_document": "R3.htm",
    "document_type": "HTML",
    "description": "IDEA: XBRL DOCUMENT",
    "purpose": "CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME"
}
```

* **Embedded images or charts**: they are saved in their original format (.jpg, .png, etc.). In `metadata.json`, the attribute `document_type` of these files is set to *GRAPHIC*.
* **4.xml files**: the raw xml file saved for filings form `4` only.

#### Usage Guide
Loading Data with Python

To process a specific company's filings, you can iterate through the folders and load the metadata first. E.g. loading primary filing file:

```Python
import json
import os

def load_filing_content(path_to_folder):
    with open(os.path.join(path_to_folder, 'metadata.json'), 'r') as f:
        meta = json.load(f)
    
    # Identify the primary document from metadata
    primary_file = next(f for f in meta['saved_files'] if f['purpose'] == 'Primary Document')
    
    with open(os.path.join(path_to_folder, primary_file['saved_as']), 'r') as f:
        return f.read()
```


#### Notes
**Sanitization**: Filenames have been sanitized to replace special characters (like /) with underscores for OS compatibility.