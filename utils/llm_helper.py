import os
import json
import tiktoken
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from openai import OpenAI
from utils.trigger_filter import filter_for_deepseek_usage

load_dotenv()

# Initialize Client Once
client = OpenAI(
    api_key=os.getenv('DEEPSEEK_API_KEY'), 
    base_url="https://api.deepseek.com"
)

def truncate_to_token_limit(text: str, limit: int = 120000) -> str:
    """
    Truncates text to fit within DeepSeek's context window (128k).
    """
    try:
        encoding = tiktoken.get_encoding("cl100k_base")
        tokens = encoding.encode(text)
        if len(tokens) <= limit: return text
        return encoding.decode(tokens[:limit])
    except Exception:
        # Fallback if tiktoken fails
        return text[:limit*4]

def query_deepseek(context_text: str, system_prompt: str, user_instruction: str, 
                   form_type: str = "generic") -> Dict[str, Any]:
    """
    Standardized DeepSeek query with Safety Filter and Token Truncation.
    """
    if not context_text or not context_text.strip(): 
        return {}

    # 1. Apply Safety Filter
    safe_text = filter_for_deepseek_usage(context_text, form_type)
    
    # 2. Apply Token Truncation
    final_text = truncate_to_token_limit(safe_text)

    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"DOCUMENT CONTEXT:\n{final_text}"}, 
                {"role": "user", "content": user_instruction}
            ],
            response_format={"type": "json_object"},
            temperature=0.0 
        )
        content = response.choices[0].message.content
        return json.loads(content) if content else {}
    except Exception as e:
        print(f"[API ERROR in {form_type}] {e}")
        return {}