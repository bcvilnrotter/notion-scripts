from ctypes import cast

from tqdm import tqdm
from utils.basic_functions import *
from utils.notion.basic_functions import *
from utils.notion.database_functions import *
from utils.notion.property_formatting import *
import pandas as pd
import requests, time

def get_institution_pages(headers,dbid):
    institutions = get_records_from_notion_database(
        headers,dbid,paginated=True)
    print_json_to_file(institutions,'institutions_raw.json')
    return institutions

def convert_to_url(text):
    text = f'%22{text.strip()}%22'
    changes = {
        '&': '%26',
        ',': '%2C',
        '/': '%2F',
        '?': '%3F',
        ':': '%3A',
        '@': '%40',
        '#': '%23',
        '$': '%24',
        '+': '%2B',
        '=': '%3D',
        ' ': '%20',
    }
    return ''.join(changes.get(char, char) for char in text)

def convert_block_to_query(block):
    return '%20OR%20'.join(convert_to_url(name) for name in block)

def build_url_perigon(data,token,limit=2000):
    first = f'https://api.perigon.io/v1/companies/all?name='
    last_piece = f'&apiKey={token}'
    data = data.copy()

    while not all(data['used']):
        unused = data[data['used'] == False]
        names = []
        last_valid_url = None
        last_valid_indices = []

        for idx,row in unused.iterrows():
            names.append(row['name'])
            query = convert_block_to_query(names)
            url = f'{first}{query}{last_piece}'

            if len(url) > limit:
                if last_valid_url is not None:
                    yield last_valid_url

                    data.loc[last_valid_indices,'used'] = True
                break

            last_valid_url = url
            last_valid_indices.append(idx)

        else:
            if last_valid_url:
                yield last_valid_url
                data.loc[last_valid_indices,'used'] = True

def pull_perigon_data(url):
    response = requests.get(url,headers={'Accept':'application/json'})
    response.raise_for_status()
    
    for item in response.json().get('results'):
        yield item

def find_perigon_matches(data,inst_names):
    
    def pro_trun_match(comp_name,lower_names):
        p_words = comp_name.split()
        
        for db_name in lower_names:
            db_words = db_name.split()
            max_trim = min(len(p_words), len(db_words))
            for trim in range(max_trim):
                p_trunc = p_words[:len(p_words)-trim]
                db_trunc = db_words[:len(db_words)-trim]
                
                if not p_trunc or not db_trunc:
                    continue
                if p_trunc == db_trunc:
                    return {'matched_name': ' '.join(db_trunc)}
        return None

    def add_string_comp_name(comp_name,lower_names):
        strings = [
            ' productions',
            ' inc.', 
            ' llc',
            ' studios',
            ' corp.'
        ]
        
        for s in strings:
            if comp_name + s in lower_names:
                return comp_name + s
        return False

    def cnnv(data):
        return sum(1 for value in data.values() if value is None)

    def add_string_lower_names(comp_name,lower_names):
        strings = [
            ', inc.',
        ]

        for s in strings:
            if comp_name in [i + s for i in lower_names]:
                return comp_name.replace(s,'')
        return False

    comp_name = data.get('name','').lower().strip()
    lower_names = [i.lower().strip() for i in inst_names]
    
    if comp_name in lower_names:
        return {'name':comp_name,'missing':cnnv(data),'record':data}
    
    ascm = add_string_comp_name(comp_name,lower_names)
    if ascm:
        return {'name':ascm,'missing':cnnv(data),'record':data}

    aslm = add_string_lower_names(comp_name,lower_names)
    if aslm:
        return {'name':aslm,'missing':cnnv(data),'record':data}

    prog_value = pro_trun_match(comp_name,lower_names)
    if prog_value is not None:
        return {'name':prog_value['matched_name'],'missing':cnnv(data),'record':data}

    name_chop = {
        i:True 
        for i in range(1,len(comp_name.split())) 
        if ' '.join(comp_name.split()[:-i]) in lower_names}
    if any(name_chop.values()):
        max_trim = max(k for k,v in name_chop.items() if v == True)
        return {'name':' '.join(
            comp_name.split()[:-max_trim]),'missing':cnnv(data),'record':data}

    return {"empty":"empty"}

def format_perigon_record(p_record,n_record,perigon_page_id):
    payload = {
        "icon": {
            "type":"external",
            "external":{
                "url":p_record.get('logo')
            }
        } if p_record.get('logo') else None,
        "properties": {},
    }

    n_prop = n_record.get('properties')

    if p_record.get('webResources'):
        if n_prop.get('Perigon.webResources.blog').get(
                'url') != p_record.get('webResources',{}).get('blog'):
            payload['properties']['Perigon.webResources.blog'] = format_notion_url(
                p_record.get('webResources',{}).get('blog'))

        if n_prop.get('Perigon.webResources.updates').get(
                'url') != p_record.get('webResources',{}).get('updates'):
            payload['properties']['Perigon.webResources.updates'] = format_notion_url(
                p_record.get('webResources',{}).get('updates'))

        if n_prop.get('Perigon.webResources.reddit').get(
                'url') != p_record.get('webResources',{}).get('reddit'):
            payload['properties']['Perigon.webResources.reddit'] = format_notion_url(
                p_record.get('webResources',{}).get('reddit'))

        if n_prop.get('Perigon.webResources.wellfound').get(
                'url') != p_record.get('webResources',{}).get('wellfound'):
            payload['properties']['Perigon.webResources.wellfound'] = format_notion_url(
                p_record.get('webResources',{}).get('wellfound'))

        if n_prop.get('Perigon.webResources.facebook').get(
            'url') != p_record.get('webResources',{}).get('facebook'):
            payload['properties']['Perigon.webResources.facebook'] = format_notion_url(
                p_record.get('webResources',{}).get('facebook'))

        if n_prop.get('Perigon.webResources.about').get(
            'url') != p_record.get('webResources',{}).get('about'):
            payload['properties']['Perigon.webResources.about'] = format_notion_url(
                p_record.get('webResources',{}).get('about'))

        if n_prop.get('Perigon.webResources.linkedin').get(
            'url') != p_record.get('webResources',{}).get('linkedin'):
            payload['properties']['Perigon.webResources.linkedin'] = format_notion_url(
                p_record.get('webResources',{}).get('linkedin'))

        if n_prop.get('Perigon.webResources.medium').get(
            'url') != p_record.get('webResources',{}).get('medium'):
            payload['properties']['Perigon.webResources.medium'] = format_notion_url(
                p_record.get('webResources',{}).get('medium'))

        if n_prop.get('Perigon.webResources.careers').get(
            'url') != p_record.get('webResources',{}).get('careers'):
            payload['properties']['Perigon.webResources.careers'] = format_notion_url(
                p_record.get('webResources',{}).get('careers'))

        if n_prop.get('Perigon.webResources.tiktok').get(
            'url') != p_record.get('webResources',{}).get('tiktok'):
            payload['properties']['Perigon.webResources.tiktok'] = format_notion_url(
                p_record.get('webResources',{}).get('tiktok'))

        if n_prop.get('Perigon.webResources.instagram').get(
            'url') != p_record.get('webResources',{}).get('instagram'):
            payload['properties']['Perigon.webResources.instagram'] = format_notion_url(
                p_record.get('webResources',{}).get('instagram'))    
    
        if n_prop.get('Perigon.webResources.youtube').get(
            'url') != p_record.get('webResources',{}).get('youtube'):
            payload['properties']['Perigon.webResources.youtube'] = format_notion_url(
                p_record.get('webResources',{}).get('youtube'))    
    
        if n_prop.get('Perigon.webResources.sitemap').get(
            'url') != p_record.get('webResources',{}).get('sitemap'):
            payload['properties']['Perigon.webResources.sitemap'] = format_notion_url(
                p_record.get('webResources',{}).get('sitemap'))

        if n_prop.get('Perigon.webResources.threads').get(
            'url') != p_record.get('webResources',{}).get('threads'):
            payload['properties']['Perigon.webResources.threads'] = format_notion_url(
                p_record.get('webResources',{}).get('threads'))

        if n_prop.get('Perigon.webResources.events').get(
            'url') != p_record.get('webResources',{}).get('events'):
            payload['properties']['Perigon.webResources.events'] = format_notion_url(
                p_record.get('webResources',{}).get('events'))

        if n_prop.get('Perigon.webResources.x').get(
            'url') != p_record.get('webResources',{}).get('x'):
            payload['properties']['Perigon.webResources.x'] = format_notion_url(
                p_record.get('webResources',{}).get('x'))

        if n_prop.get('Perigon.webResources.wikipedia').get(
            'url') != p_record.get('webResources',{}).get('wikipedia'):
            payload['properties']['Perigon.webResources.wikipedia'] = format_notion_url(
                p_record.get('webResources',{}).get('wikipedia'))

    if p_record.get('address') is not None and n_prop.get(
        'Perigon.address',{}).get('rich_text') != [{'text':{
            'content':p_record.get('address')
        }}]:
        payload['properties']['Perigon.address'] = format_notion_text(
            p_record.get('address'))

    if n_prop.get('Perigon.monthlyVisits').get(
        'number') != p_record.get('monthlyVisits'):
        payload['properties']['Perigon.monthlyVisits'] = format_notion_number(
            p_record.get('monthlyVisits'))
    
    if n_prop.get('Perigon.updatedAt').get(
        'date') != p_record.get('updatedAt'):
        payload['properties']['Perigon.updatedAt'] = format_notion_date(
            p_record.get('updatedAt'))
    
    ceo = p_record.get('ceo')
    if ceo is not None and n_prop.get('Perigon.ceo',{}).get('select') != {'name':ceo}:
        payload['properties']['Perigon.ceo'] = format_notion_select(ceo)

    """
    if n_prop.get('Perigon.ceo').get(
        'select') != {'name':p_record.get('ceo')}:
        payload['properties']['Perigon.ceo'] = format_notion_select(
            p_record.get('ceo'))
    """
    
    if n_prop.get('Perigon.fullTimeEmployees').get(
        'number') != p_record.get('fullTimeEmployees'):
        payload['properties']['Perigon.fullTimeEmployees'] = format_notion_number(
            p_record.get('fullTimeEmployees'))
    
    if n_prop.get('Perigon.zip').get('select') != {'name':p_record.get('zip')}:
        payload['properties']['Perigon.zip'] = format_notion_select(
            p_record.get('zip'))
    
    if n_prop.get('Perigon.cusip').get(
        'rich_text') != [{'text':{'content':p_record.get('cusip')}}]:
        payload['properties']['Perigon.cusip'] = format_notion_text(
            p_record.get('cusip'))
    
    if n_prop.get('Perigon.naics').get('number') != p_record.get('naics'):
        payload['properties']['Perigon.naics'] = format_notion_number(
            p_record.get('naics'))
    
    if n_prop.get('Perigon.globalRank').get('number') != p_record.get('globalRank'):
        payload['properties']['Perigon.globalRank'] = format_notion_number(
            p_record.get('globalRank'))
    
    if n_prop.get('Perigon.sic').get('number') != p_record.get('sic'):
        payload['properties']['Perigon.sic'] = format_notion_number(
            p_record.get('sic'))
    
    if n_prop.get('Perigon.isin').get(
        'rich_text') != [{'text':{'content':p_record.get('isin')}}]:
        payload['properties']['Perigon.isin'] = format_notion_text(
            p_record.get('isin'))
    
    if n_prop.get('Perigon.isActivelyTrading').get(
        'checkbox') != p_record.get('isActivelyTrading'):
        payload['properties']['Perigon.isActivelyTrading'] = format_notion_checkbox(
            p_record.get('isActivelyTrading'))
    
    if n_prop.get('Perigon.cik').get(
        'rich_text') != [{'text':{'content':p_record.get('cik')}}]:
        payload['properties']['Perigon.cik'] = format_notion_text(
            p_record.get('cik'))
    
    if n_prop.get('Perigon.isEtf').get(
        'checkbox') != p_record.get('isEtf'):
        payload['properties']['Perigon.isEtf'] = format_notion_checkbox(
            p_record.get('isEtf'))
    
    if n_prop.get('Perigon.state').get('select') != {'name':p_record.get('state')}:
        payload['properties']['Perigon.state'] = format_notion_select(
            p_record.get('state'))
    
    if n_prop.get('Perigon.yearFunded').get(
        'number') != p_record.get('yearFunded'):
        payload['properties']['Perigon.yearFunded'] = format_notion_number(
            p_record.get('yearFunded'))
    
    if n_prop.get('Perigon.isFund').get(
        'checkbox') != p_record.get('isFund'):
        payload['properties']['Perigon.isFund'] = format_notion_checkbox(
            p_record.get('isFund'))
    
    if n_prop.get('Perigon.isAdr').get(
        'checkbox') != p_record.get('isAdr'):
        payload['properties']['Perigon.isAdr'] = format_notion_checkbox(
            p_record.get('isAdr'))
    
    if n_prop.get('Perigon.city').get('select') != {'name':p_record.get('city')}:
        payload['properties']['Perigon.city'] = format_notion_select(
            p_record.get('city'))
    
    if n_prop.get('Perigon.country').get('select') != {'name':p_record.get('country')}:
        payload['properties']['Perigon.country'] = format_notion_select(
            p_record.get('country'))
   
    if n_prop.get('Perigon.industry').get('select') != {'name':p_record.get('industry')}:
        industry = p_record.get('industry')
        if industry:
            industry_list = [i.strip() for i in industry.split(',')]
            payload['properties']['Perigon.industry'] = format_notion_multi_select(
            industry_list)
    
    if n_prop.get('Perigon.revenue').get(
        'rich_text') != [{'text':{'content':p_record.get('revenue')}}]:
        payload['properties']['Perigon.revenue'] = format_notion_text(
            p_record.get('revenue'))
    
    if n_prop.get('Perigon.description').get(
        'rich_text') != [{'text':{'content':p_record.get('description')}}]:
        payload['properties']['Perigon.description'] = format_notion_text(
            p_record.get('description'))
    
    if n_prop.get('Perigon.sector').get('select') != {'name':p_record.get('sector')}:
        payload['properties']['Perigon.sector'] = format_notion_select(
            p_record.get('sector'))

    payload['properties'] = {
        k:v for k,v in payload['properties'].items()
        if v not in (None,{"rich_text":[]},{"select":None},{'url':None})
    }

    payload['properties']['Enriched By'] = format_notion_single_relation(
        perigon_page_id)
    
    return {k:v for k,v in payload.items() if v is not None}

def update_record(headers,dbid,page_id,data,list_of_data):
    best_record = min(
        [i for i in list_of_data if i.get('name') == data],
        key=lambda x: x.get('missing',float('inf')))
    
    br_notion = search_for_notion_page_by_title(
        headers=headers,
        dbid=dbid,
        title=best_record['name'],
        lower_case=True)
    
    if not br_notion:
        return False

    p_record = format_perigon_record(
        p_record=best_record['record'],
        n_record=get_notion_page_data(headers,br_notion),
        perigon_page_id=page_id)

    update_entry_to_notion_database(
        headers=headers,
        data=p_record,
        page_id=br_notion
    )
    
    return True


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