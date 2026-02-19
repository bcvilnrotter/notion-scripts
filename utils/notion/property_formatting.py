import pandas as pd
from datetime import datetime

def format_notion_number(number):
    if number is None:
        return {"number":None}
    return {"number": int(number)}

def format_notion_number_outer(number_format):
    return format_notion_number(number_format.get('number'))

def format_notion_text(text):
    if text is None:
        return {"rich_text": []}
    return {"rich_text": [{"text": {"content": text}}]}

def format_notion_rich_text_outer(text_format):
    return format_notion_text(text_format['rich_text'][0]['plain_text'] if text_format['rich_text'] != [] else '')

def format_notion_title_outer(title_format):
    return {"title": [
        {
            "text": {
                "content": title_format.get('title')[0].get('plain_text')
            }
        }
    ]}

def format_notion_date(start, end=None, patterns=None, is_datetime=False,string_pattern=None):
    if start is None:
        return {"date":None}
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
        start = start.strftime(string_pattern) if string_pattern else start.strftime('%Y-%m-%d')
        if end:
            assert isinstance(end,datetime)
            end = end.strftime(string_pattern) if string_pattern else end.strftime('%Y-%m-%d')
    if string_pattern and is_datetime is not True:
        start = pd.to_datetime(start,utc=True).strftime(string_pattern)
        end = pd.to_datetime(end,utc=True).strftime(string_pattern) if end != None else None
    return {"date": {"start": start, "end": end}}

def format_notion_date_outer(date_format):
    return format_notion_date(date_format.get('date').get('start'),end=date_format.get('date').get('end'),string_pattern='%Y-%m-%dT%H:%M')

def format_notion_single_relation(page_id):
    return {"relation": [{"id": page_id}]}

def format_notion_multi_relation(page_ids):
    return {"relation": [{"id": page_id} for page_id in page_ids]}

def format_notion_select(selection):
    if selection is None:
        return {"select":None}
    return {"select": {"name": selection}}

def format_notion_select_outer(select_format):
    return format_notion_select(select_format.get('select').get('name')) if select_format.get('select') != None else format_notion_select(select_format.get('select'))

def format_notion_multi_select(selections):
    if not selections:
        return {"multi_select":[]}
    return {"multi_select": [{
        "name": selection} 
        for selection in [v.replace(',','') for v in selections]]}

def format_notion_checkbox(checked):
    if checked is None:
        return {"checkbox":False}
    return {"checkbox": checked}

def format_notion_checkbox_outer(checkbox_format):
    return format_notion_checkbox(checkbox_format.get('checkbox'))

def format_notion_url(url):
    return {"url": url}

def format_notion_url_outer(url_format):
    return format_notion_url(url_format.get('url'))

def format_notion_place(lat,long,name,address):
    return {"place":{
        "lat":lat,
        "long":long,
        "name":name,
        "address":address
    }}

def get_name_from_notion_page(page):
    return page.get(
        'properties').get('Name').get('title')[0].get('text').get('content')

def dynamic_notion_formatter(type_name,value,exclude=['relation']):
    if type_name in exclude:
        return ''
    
    FORMATTERS = {
        "rich_text": format_notion_rich_text_outer,
        "number": format_notion_number_outer,
        "date": format_notion_date_outer,
        "select": format_notion_select_outer,
        "checkbox": format_notion_checkbox_outer,
        "url": format_notion_url_outer,
        "title":format_notion_title_outer,
    } 
    
    return FORMATTERS[type_name](value)