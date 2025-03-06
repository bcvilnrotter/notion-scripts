#%%
import requests,os,json
from dotenv import load_dotenv

# %%
def get_secret(secret_key):
    if not os.getenv(secret_key):
        env_path = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)),'..\..','.gitignore\.env'))
        load_dotenv(dotenv_path=env_path)

    value = os.getenv(secret_key)
    print("".join(['*'])*len(value))
    if value is None:
        ValueError(f"Secret '{secret_key} not found.")
    
    return value

# %%
def get_banner_url_from_appid(appid):
    game_url = f'https://store.steampowered.com/api/appdetails?appids={appid}'
    response = requests.get(game_url,stream=True)
    if response.json().get(appid).get('success'):
        return json.loads(response.text).get(appid).get('data').get('header_image')
    print(f'AppId data not found: {response.text}')
    return {}
#%%
def get_all_page_atts(headers,database_id):
    url = f'https://api.notion.com/v1/databases/{database_id}/query'
    response = requests.post(url,headers=headers)

    if response.status_code == 200:
        pages = response.json().get('results',[])
        return {
            page["id"]:page['properties']['appId']['rich_text'][0]['text']['content'] 
            for page in pages
            if page['properties']['appId'].get('rich_text')
        }
    else:
        print(f'Error fetching pages:',response.json())
        return {}

# %%
def add_image_cover_all_records():
    print('collecting keys.')
    keys = ['NOTION_TOKEN','NOTION_VIDEO_GAME_STATS_DBID']
    key_chain = {key:get_secret(key) for key in keys}

    headers = {
        'Authorization': f"Bearer {key_chain['NOTION_TOKEN']}",
        'Content-Type': 'application/json',
        'Notion-Version': '2022-06-28'
    }

    #print(get_all_page_atts(headers,key_chain['NOTION_VIDEO_GAME_STATS_DBID']))
    for page_id,appId in get_all_page_atts(headers,key_chain['NOTION_VIDEO_GAME_STATS_DBID']).items():
        page_url = f'https://api.notion.com/v1/pages/{page_id}'
        print(f'Setting up adjustments to page: {page_id}')
        data = {
            'cover': {
                'type': 'external',
                'external': {'url': get_banner_url_from_appid(appId)}
            }
        }

        print(f'Posting cover art to appId: {appId}')
        response = requests.patch(page_url,headers=headers, json=data)

        if response.status_code == 200:
            print(f'Updated cover for page: {page_id}')
        else:
            print(f'Error updated page {page_id}:', response.json())

# %%
