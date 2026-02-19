import json
import pandas as pd
import numpy as np

def get_notion_header(key_chain):
    return {
        'Authorization': f"Bearer {key_chain['NOTION_TOKEN']}",
        'Content-Type': 'application/json',
        'Notion-Version': '2022-06-28'
    }

def get_notion_header_scalable(
        notion_token,
        ctype='application/json',
        notion_version='2022-06-28'):
    return {
        'Authorization': f"Bearer {notion_token}",
        'Content-Type': ctype,
        'Notion-Version': notion_version
    }

def prop_value_is_missing(prop):
    try:
        if pd.isna(prop):
            return True
    except(ValueError, TypeError):
        pass
    if isinstance(prop, (list, np.ndarray)) and len(prop) == 0:
        return True
    if prop in ['null', 'NaN', None]:
        return True
    if prop == '':
        return True
    return False

def try_notion_payload(payload):
    for i in payload:
        try:
            json.dumps(i, ensure_ascii=False, allow_nan=False)
        except ValueError as e:
            return False, i, str(e)
    return True, None, None
