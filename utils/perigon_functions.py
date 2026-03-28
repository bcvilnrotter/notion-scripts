from ctypes import cast

from tqdm import tqdm
from utils.basic_functions import *
from utils.notion.basic_functions import *
from utils.notion.database_functions import *
from utils.notion.property_formatting import *
from utils.perigon.perigon_basic_functions import *
from utils.perigon.perigon_institutions_functions import *

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
    
    url_list = [i for i in build_url_perigon(inm,keychain[perigon_token])]
    perigon_json = [
        record 
        for url in url_list
        for record in pull_perigon_data(url)
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
