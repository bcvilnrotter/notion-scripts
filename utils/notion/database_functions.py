import requests, json, time, threading
from requests.adapters import HTTPAdapter
from requests.exceptions import SSLError, ConnectionError, Timeout

def get_notion_database_info(headers,database_id,
                             url='https://api.notion.com/v1/databases/'):
    return requests.get(url+database_id,headers=headers)

def get_notion_page_data(headers,pageid):
    response = requests.get(
        f'https://api.notion.com/v1/pages/{pageid}',headers=headers)
    response.raise_for_status()
    return response.json()

def get_notion_page_name(headers,pageid):
    response = requests.get('https://api.notion.com/v1/pages/{}'.format(
        pageid=pageid),headers=headers)
    response.raise_for_status()
    return response.json().get(
        'properties').get(
            'Name').get('title')[0].get('text').get('content')

def get_notion_page_id(page_data):
    return page_data.get('id') if page_data else "empty"
    
def search_for_notion_page_by_title(
        headers,dbid,title,
        prop_name="Name",
        lower_case=False):
    query_url = f"https://api.notion.com/v1/databases/{dbid}/query"
    title_formatted = title.lower() if lower_case else title
    
    payload = {
        "filter": {
            "property": prop_name,
            "title": {
                "equals": title_formatted
            }
        }
    }

    response = requests.post(query_url,headers=headers,json=payload)
    if response.status_code == 200 and response.json()['results'] != []:
        return response.json()["results"][0]["id"]
    else:
        return False

def search_for_notion_page_by_datetime(headers,dbid,datetime):
    query_url = f"https://api.notion.com/v1/databases/{dbid}/query"

    payload = {
        "filter": {
            "property": "datetime",
            "date": {
                "equals": datetime
            }
        }
    }

    response = requests.post(query_url,headers=headers,json=payload)
    if response.status_code == 200 and response.json()['results'] != []:
        return response.json()["results"][0]["id"]
    else:
        return False

def archive_page_from_database(headers,page_id):
    response = requests.patch(
        f'https://api.notion.com/v1/pages/{page_id}',
        headers=headers,data={'archived':True})
    
    response.raise_for_status()
    return response

def update_entry_to_notion_database(headers,data,page_id):
    
    try:
        response = requests.patch(
            f'https://api.notion.com/v1/pages/{page_id}',
            headers=headers, json=data)

        response.raise_for_status()
        return response
    except Exception as e:
        print(json.dumps(data,indent=2))
        print(response.status_code)
        print(response.headers)
        print(response.json())

session = requests.Session()
adapter = HTTPAdapter(
    pool_connections=10,
    pool_maxsize=10,
    pool_block=True
)
session.mount('https://', adapter)

_rate_lock = threading.Lock()
_last_request_time = [0.0]
MIN_INTERVAL = 0.4

def _throttle():
    with _rate_lock:
        now = time.time()
        wait = MIN_INTERVAL - (now - _last_request_time[0])
        if wait > 0:
            time.sleep(wait)
        _last_request_time[0] = time.time()

def upsert_entry_to_notion_database(
        headers,data,page_id) -> requests.Response:
    _throttle()
    response = None
    for attempt in range(5):
        try:
            if page_id != "empty":
                response = session.patch(
                    f'https://api.notion.com/v1/pages/{page_id}',
                    headers=headers, json=data.get('properties'),
                    timeout=30)            

            else:
                response = session.post(
                    'https://api.notion.com/v1/pages',
                    headers=headers, json=data,timeout=30)
            
            if response.status_code == 429:
                wait = float(
                    response.headers.get(
                        'Retry-After', 2 ** attempt))
                
                ret_aft = response.headers.get('Retry-After')
                note = f"[429] attempt {attempt}, waiting {wait}s"
                note += f", Retry-After={ret_aft}"
                print(ret_aft)
                
                time.sleep(wait)
                continue
            
            response.raise_for_status()
            return response
        
        except (SSLError, ConnectionError, Timeout) as e:
            print(f"[net retry {attempt}] {type(e).__name__}: {e}")
            time.sleep(2 ** attempt)
            continue
        
        except requests.HTTPError:
            if response is not None:
                err_msg = f"[HTTP error {response.status_code}]"
                err_msg += f" attempt {attempt}:"
                err_msg += f" {response.text[:200]}"
                print(err_msg)
            raise
    
    res = response.status_code if response else '?'
    rte_msg = "Notion upsert failed after 5 retries"
    rte_msg += f" (last status: {res})"
    raise RuntimeError(rte_msg)

def get_new_empty_notion_page(dbid,title):
    return {
        'parent': {'database_id': dbid},
        'properties': {
            "Name": {
                "title":[{
                    "text":{
                        "content":title
                    }
            }]
        }
    }
}

def new_entry_to_notion_database(headers,data):
    response = requests.post(
        'https://api.notion.com/v1/pages',
        headers=headers, json=data)
    response.raise_for_status()
    name = response.json().get(
        'properties').get(
            'Name').get('title')[0].get('text').get('content')
    print(f' ... Created new page in database {data} with name "{name}"')
    return response

def get_records_from_notion_database(header,database_id,paginated=False):
    url = f'https://api.notion.com/v1/databases/{database_id}/query'
    response = requests.post(url,headers=header)
    response.raise_for_status()
    if paginated:
        return request_paginated_data(url,header)
    return response

def request_paginated_data(url,header):
    all_data = []
    has_more = True
    next_cursor = None

    while has_more:
        payload = {'start_cursor':next_cursor} if next_cursor else {}
        response = requests.post(url,headers=header,json=payload).json()
        all_data.extend(response.get('results',[]))

        has_more = response.get('has_more')
        next_cursor = response.get('next_cursor')
    
    print('... Returning {} total records from paginated request.'.format(
        len(all_data)))
    return all_data

def get_page_name(header,page_id):
    url = f'https://api.notion.com/v1/pages/{page_id}'
    response = requests.get(url,headers=header).json()
    return response.get(
        'properties').get('Name').get('title')[0].get('text').get('content')

def get_page_update_data(dbid,name,properties_dict,cover_image_url=None):
    data = {'parent':{'database_id':dbid}}
    if cover_image_url:
        data['cover']= {
            "type":"external",
            "external":{
                "url":cover_image_url
            }
        }
    data['properties'] = {
        "Name": {
            "title": [{"text": {"content": name}}]},**properties_dict}
    return data
