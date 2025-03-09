#%%
import requests,os,json
from datetime import datetime, timedelta
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
def get_keychain(keys):
    return {key:get_secret(key) for key in keys}

def get_notion_header(key_chain):
    return {
        'Authorization': f"Bearer {key_chain['NOTION_TOKEN']}",
        'Content-Type': 'application/json',
        'Notion-Version': '2022-06-28'
    }

def add_image_cover_all_records():
    print('collecting keys.')
    key_chain = get_keychain(['NOTION_TOKEN','NOTION_VIDEO_GAME_STATS_DBID'])
    print('generate header')
    headers = get_notion_header(key_chain)

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
def pull_data_from_steam():
    steam_url = f"https://api.steampowered.com/IPlayerService/GetRecentlyPlayedGames/v0001/?key={get_secret('STEAM_KEY')}&steamid={get_secret('STEAM_USER')}&format=json"
    recent_playtime = requests.get(steam_url)
    return recent_playtime.json()

# %%
def format_2week_playtime_to_notion_data(game_data):
    today = datetime.utcnow().strftime('%Y-%m-%d')
    two_weeks_ago = (datetime.utcnow() - timedelta(days=14)).strftime('%Y-%m-%d')
    properties = {
        "Title": {"title": [{"text": {"content": game_data['name']}}]},
        "AppId": {"rich_text": [{"text": {"content": str(game_data['appid'])}}]},
        "Date Range": {"date": {"start": two_weeks_ago, "end":today}},
        "playtime_2weeks": {"number": game_data['playtime_2weeks']},
        "playtime_forever": {"number": game_data['playtime_forever']},
        "img_icon_url": {"rich_text":[{"text": {"content": game_data['img_icon_url']}}]},
        "playtime_windows_forever": {"number": game_data['playtime_windows_forever']},
        "playtime_mac_forever": {"number": game_data['playtime_mac_forever']},
        "playtime_linux_forever": {"number": game_data['playtime_linux_forever']},
        "playtime_deck_forever": {"number": game_data['playtime_deck_forever']}
    }

    return {
        "parent": {"database_id": get_secret('NOTION_RAW_PLAYTIME_DBID')},
        "properties": properties
    }

# %%
def upload_2week_playtime_to_notion_database():
    print('collecting keys.')
    key_chain = get_keychain(['NOTION_TOKEN','NOTION_RAW_PLAYTIME_DBID'])
    print('generate header')
    headers = get_notion_header(key_chain)    
    for record in pull_data_from_steam().get('response').get('games'):
        response = requests.post('https://api.notion.com/v1/pages',headers=headers, data=json.dumps(format_2week_playtime_to_notion_data(record)))
        if response.status_code == 200:
            print(f"successfully added {record.get('name')} to Notion.")
        else:
            print(f"failed to add {record.get('name')} - {response.json()}")

# %%
