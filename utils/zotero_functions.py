import pathlib, sys, json
from utils.basic_functions import *
from utils.notion.basic_functions import *
from utils.notion.database_functions import *
from utils.notion.property_formatting import *

def get_page_name_string(record):
    return record.get('properties').get('Title').get('rich_text')[0].get('text').get('content')

def get_author_list_from_record(record):
    return record.get('properties').get('Authors').get('rich_text')[0].get('plain_text')

def convert_name_string(name):
    if ',' in name:
        last, first = [part.strip() for part in name.split(', ')]
        return f"{first} {last}"
    else:
        return name
    
def check_and_operate_on_author_page(headers,dbid,name):
    page_id = search_for_notion_page_by_title(headers,dbid,name)
    if not page_id:
        return new_entry_to_notion_database(headers,get_new_empty_notion_page(dbid,name)).json().get('id')
    else:
        return page_id

def validate_author_records():
    keychain = get_keychain(['NOTION_TOKEN','NOTION_ZOTERO_DBID','NOTION_AUTHORS_DBID'])   
    url = f'https://api.notion.com/v1/databases/{keychain["NOTION_ZOTERO_DBID"]}/query'
    headers = get_notion_header(keychain)
    records = request_paginated_data(url,headers)
    
    for record in records:
        title = get_page_name_string(record)
        shrt_title = title[:50] + ('...' if len(title) > 50 else '')
        name = get_page_name(headers,record.get('id'))
        author_list = {author for author in get_author_list_from_record(record).split('\n')}

        author_ids = record.get('properties').get("Authors  DB").get('relation')
        authors_dict = {get_page_name(headers,author_id.get('id')):author_id for author_id in author_ids}
        authors_dict_checked = {name:author_id for name,author_id in authors_dict.items() if name in author_list}
        missing_authors = [name for name in author_list if name not in authors_dict_checked.keys()]

        authors_ids_list = [id_dict.get('id') for _,id_dict in authors_dict_checked.items()]
        authors_ids_list.extend(
            [
                check_and_operate_on_author_page(headers,keychain['NOTION_AUTHORS_DBID'],name)
                for name in missing_authors
            ])

        if authors_ids_list != [] and len(authors_ids_list) != len(author_ids):
            try:
                response = update_entry_to_notion_database(
                    headers,
                    get_page_update_data(
                        keychain['NOTION_ZOTERO_DBID'],
                        name,
                        {"Authors  DB": format_notion_multi_relation(authors_ids_list)}
                    ),
                    record.get('id')
                )
                response.raise_for_status()
                print(f'... Updated {len(authors_ids_list)} authors for "{shrt_title}"')
            except Exception as e:
                print(f'... Error updating "{shrt_title}"\n{e}')
        else:
            print(f'... No update needed for "{shrt_title}"')