from utils.basic_functions import *
from utils.notion.basic_functions import *
from utils.notion.sessions_functions import *
from utils.notion.database_functions import *
from utils.notion.property_formatting import *
from utils.perigon.perigon_basic_functions import *
from utils.perigon.perigon_institutions_functions import *
from utils.perigon.perigon_stories_functions import *

from ctypes import cast
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

def enrich_institutions_perigon(institutions_dbid,perigon_token,perigon_app_id):
    keychain = get_keychain(
        ['NOTION_TOKEN',institutions_dbid,perigon_token,perigon_app_id])
    headers = get_notion_header_scalable(keychain['NOTION_TOKEN'])

    institutions = get_institution_pages(headers,keychain[institutions_dbid])    
    inm = pd.DataFrame({
        'length': [len(get_name_from_notion_page(i)) for i in institutions],
        'name': [get_name_from_notion_page(i) for i in institutions],
        'used': [False for i in institutions]
    })
    
    url_base = 'https://api.perigon.io/v1/companies/all?'
    url_base += 'size=100'
    url_base += '&page=0'
    url_base += '&name='
    
    url_list = [
        i 
        for i in build_url_perigon(inm,keychain[perigon_token],first=url_base)]
    print(f'... Built {len(url_list)} Perigon API URLs for institution retrieval.')
    
    perigon_json = [
        record 
        for url in url_list
        for record in pull_perigon_data(url,verbose=True)
        ]

    print(f'... Pulled {len(perigon_json)}'
          f' records from Perigon data.')

    inst_names = [get_name_from_notion_page(i) for i in institutions]
    perigon_filtered = [find_perigon_matches(i,inst_names) for i in perigon_json]
    perigon_filtered = [i for i in perigon_filtered if 'empty' not in i]
    
    print(f'... Filtered Perigon data to {len(perigon_filtered)}'
            f' records matching institution names.')

    perigon_filtered_dup = {i.get('name') for i in perigon_filtered}
    results = [
        (j,update_record(
            headers=headers,
            dbid=keychain[institutions_dbid],
            page_id=keychain[perigon_app_id],
            data=j,
            list_of_data=perigon_filtered))
        for j in tqdm(
            perigon_filtered_dup,total=len(perigon_filtered_dup),
            desc='... Enriching Notion records with Perigon data.')
    ]

    failed = [name for name,success in results if not success]
    if failed:
        print(f'... {len(failed)} failed records:')
        for name in failed:
            print(f'    - {name}')

def pull_stories_by_institution(
        institution_dbid, stories_dbid,perigon_token,perigon_app_id):
    
    keychain = get_keychain(
        ['NOTION_TOKEN',institution_dbid,stories_dbid,
         perigon_token,perigon_app_id])
    headers = get_notion_header_scalable(keychain['NOTION_TOKEN'])

    institutions = get_institution_pages(headers,keychain[institution_dbid])
    stories_pages = get_stories_pages(headers,keychain[stories_dbid])
    inm = pd.DataFrame({
        'length': [len(get_perigon_id_from_notion_page(i)) for i in institutions],
        'name': [get_perigon_id_from_notion_page(i) for i in institutions],
        'used': [False for i in institutions]
    })

    create_time = pd.Timestamp.now() - pd.DateOffset(months=1)
    url_base = 'https://api.perigon.io/v1/stories/all?'
    url_base += f'initializedFrom={create_time.strftime("%Y-%m-%d")}'
    url_base += f'&updatedFrom={create_time.strftime("%Y-%m-%d")}'
    url_base += '&showNumResults=true'
    url_base += '&size=100'
    url_base += '&page=0'
    url_base += '&companyId='
    
    url_list = [i for i in build_url_perigon(
        inm[inm['length'] > 0],
        keychain[perigon_token],first=url_base,stories=True)]

    print(url_list)
    print(f'... Built {len(url_list)} Perigon API URLs for story retrieval.')
    
    perigon_json = [
        record 
        for url in url_list
        for record in pull_perigon_data(url,verbose=True)
        ]
    
    print(f'... Pulled {len(perigon_json)}'
            f' records from Perigon data.')
    
    """
    nowTime = pd.Timestamp.now().strftime("%Y_%m_%d_%H_%M_%S")
    print_json_to_file(perigon_json,f'perigon_stories_{nowTime}.json')
    
    from pathlib import Path
    
    filename = 'perigon_stories_2026_04_09_10_34_02.json'
    with open(Path(__file__).parent.parent / "tmp" / filename,"r") as f:
        perigon_json = json.load(f)

    print(len(perigon_json))

    institution_pages = get_institution_pages(headers,keychain[institution_dbid])
    print_json_to_file(stories_pages,'stories_pages.json')
    
    notion_records = [
        format_perigon_story_record(
            perigon_record=i,
            stories_dbid=keychain[stories_dbid],
            institutions_pages=institution_pages,
            headers=headers,
            perigon_pid=keychain[perigon_app_id]
        ) for i in perigon_json
    ]
    """

    notion_records = update_record_stories(
        perigon_pages=perigon_json,
        stories_dbid=keychain[stories_dbid],
        institutions_pages=institutions,
        stories_pages=stories_pages,
        perigon_pid=keychain[perigon_app_id]
    )

    print_string = f"... Formatted {len(notion_records)}"
    print_string += " Notion records from Perigon data."
    print(print_string)

    notion_new_records = [[i,j] for [i,j] in notion_records if i == "empty"]
    notion_updated_records = [[i,j] for [i,j] in notion_records if i != "empty"]
    print_string2 = f"... > Of these, {len(notion_new_records)} are new records,"
    print(print_string2)

    #print_json_to_file(notion_new_records,'notion_new_records.json')

    print_string3 = f"...   and {len(notion_updated_records)} are updated records."
    print(print_string3)

    #print_json_to_file(notion_updated_records,'notion_updated_records.json')

    """
    nowTime = pd.Timestamp.now().strftime("%Y_%m_%d_%H_%M_%S")
    print_json_to_file(
        notion_records,
        f'notion_formatted_stories_{nowTime}.json')
    """

    def _upsert_one(page_id,data):
        name = data.get(
            'properties').get('Name').get('title')[0].get('text').get('content')
        status = upsert_entry_to_notion_database(
            headers=headers,
            data=data,page_id=page_id).status_code
        return (name,status)

    with ThreadPoolExecutor(max_workers=1) as ex:
        futures = [ex.submit(
            _upsert_one,page_id=i,data=j) for [i,j] in notion_new_records]
        results = [
            future.result() for future in tqdm(
                as_completed(futures),total=len(futures),
                desc='... Uploading Perigon stories to Notion.'
            )
        ]