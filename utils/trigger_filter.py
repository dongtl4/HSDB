import re

def filter_for_deepseek_usage(text: str, form_type: str = "generic") -> str:
    """
    Sanitizes text to prevent DeepSeek API 'Content Exists Risk' errors.
    1. Truncates dangerous sections (Shareholder Proposals).
    2. Redacts specific high-risk keywords that trigger safety filters.
    """
    if not text: return ""

    # 1. SECTION STRIPPING (Remove large chunks of risky content)
    if form_type == "DEF 14A":
        # Proxy Statements: The data we need is always in Items 1-3.
        # "Item 4" or "Shareholder Proposals" is where the ESG/Political triggers live.
        # We cut the text off at the first sign of these sections.
        cut_triggers = [
            r"Item\s+4\.", 
            r"ITEM\s+4\.", 
            r"Shareholder\s+Proposals?", 
            r"Stockholder\s+Proposals?",
            r"Proposals?\s+of\s+Shareholders"
        ]
        
        earliest_idx = len(text)
        for trigger in cut_triggers:
            match = re.search(trigger, text, re.IGNORECASE)
            if match:
                # Only cut if it's in the second half of the doc (avoid TOC matches)
                if match.start() > len(text) * 0.1: 
                    earliest_idx = min(earliest_idx, match.start())
        
        if earliest_idx < len(text):
            print(f"[SAFETY] Truncated Proxy at index {earliest_idx} (detected '{text[earliest_idx:earliest_idx+20]}...')")
            text = text[:earliest_idx]

    # 2. KEYWORD REDACTION (The "Nuclear Option")
    # These keywords are common triggers for "Content Risk" in Chinese LLMs.
    # We replace them with [REDACTED] to preserve the sentence structure.
    risk_keywords = [
        "Taiwan", "Hong Kong", "Xinjiang", "Uyghur", "Tibet", 
        "Human Rights", "Civil Rights", "Discrimination", "Political Spending", 
        "Lobbying", "Climate Change", "Abortion", "Reproductive Rights", 
        "Military", "Weapons", "Conflict Minerals"
    ]
    
    # Compile a massive regex for speed
    pattern = re.compile(r'\b(' + '|'.join(map(re.escape, risk_keywords)) + r')\b', re.IGNORECASE)
    
    text, count = pattern.subn("[SAFE_REDACTED]", text)
    
    if count > 0:
        print(f"[SAFETY] Redacted {count} sensitive keywords from {form_type}.")

    return text