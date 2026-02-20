import requests, json

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
        headers=headers, data=json.dumps(data))
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
