from datetime import datetime

def format_notion_number(number):
    return {"number": number}

def format_notion_text(text):
    return {"rich_text": [{"text": {"content": text}}]}

def format_notion_date(start, end=None, patterns=None, is_datetime=False):
    if patterns:
        for pattern in patterns:
            try:
                start = datetime.strptime(start, pattern).strftime('%Y-%m-%d')
                if end: 
                    end = datetime.strptime(end, pattern).strftime('%Y-%m-%d')
            except:
                continue
    if is_datetime:
        assert isinstance(start,datetime)
        start = start.strftime('%Y-%m-%d')
        if end:
            assert isinstance(end,datetime)
            end = end.strftime('%Y-%m-%d')
    return {"date": {"start": start, "end": end}}

def format_notion_single_relation(page_id):
    return {"relation": [{"id": page_id}]}

def format_notion_multi_relation(page_ids):
    return {"relation": [{"id": page_id} for page_id in page_ids]}

def format_notion_select(selection):
    return {"select": {"name": selection}}

def format_notion_multi_select(selections):
    return {"multi_select": [{"name": selection} for selection in [v.replace(',','') for v in selections]]}

def format_notion_checkbox(checked):
    return {"checkbox": checked}

def format_notion_url(url):
    return {"url": url}