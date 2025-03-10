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
def get_duolingo_api():
    duolingo_url = f"https://www.duolingo.com/2017-06-30/users/{get_secret('DUOLINGO_USER')}"
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": "https://www.duolingo.com/",
    "X-Requested-With": "XMLHttpRequest"
    }
    responses = requests.get(duolingo_url,headers=headers)
    duolingo_list=[
        'totalXp',
        'picture',
        'courses',
        'streak',
        'streakData',
        'subscriberLevel'
    ]
    return {content:responses.json().get(content) for content in duolingo_list}

# %%
def duolingo_data_notion_format(data):
    page_properties = {
        'totalXp': {"number": data['totalXp']},
        'picture': {"rich_text": [{"text": {"content": data['picture']}}]},
        'streak': {"number": data['streak']},
        'subscriberLevel':{"rich_text": [{"text": {"content": data['subscriberLevel']}}]}
    }
    return {"properties": page_properties},data['courses'],data['streakData']

def duolingo_data_notion_courses_format(dbid,data):
    return {
        'parent': {'database_id': dbid},
        'properties': {
            "Name": {"title": [{"text": {"content": data['title']}}]},
            'authorId': {"rich_text": [{"text": {"content": data['authorId']}}]},
            'fromLanguage': {"rich_text": [{"text": {"content": data['fromLanguage']}}]},
            'healthEnabled': {"checkbox": data['healthEnabled']},
            'id': {"rich_text": [{"text": {"content": data['id']}}]},
            'learningLanguage': {"rich_text": [{"text": {"content": data['learningLanguage']}}]},
            'placementTestAvailable': {"checkbox": data['placementTestAvailable']},
            'preload': {"checkbox": data['preload']},
            'xp': {"number": data['xp']},
            'crowns': {"number": data['crowns']}
        }
    }

def search_for_notion_page_by_title(headers,dbid,title):
    query_url = f"https://api.notion.com/v1/databases/{dbid}/query"

    payload = {
        "filter": {
            "property": "Name",
            "title": {
                "equals": title
            }
        }
    }

    response = requests.post(query_url,headers=headers,json=payload)
    if response.status_code == 200:
        return response.json()["results"][0]["id"]
    else:
        return False

def upload_duolingo_data_to_notion():
    key_list=[
        'NOTION_TOKEN',
        'DUOLINGO_USER',
        'DUOLINGO_COURSES_PAGEID',
        'DUOLINGO_STREAK_DATA_DBID',
        'DUOLINGO_COURSES_DBID'
    ]
    keychain = get_keychain(key_list)
    headers = get_notion_header(keychain)
    page_json,courses,streakData = duolingo_data_notion_format(get_duolingo_api())

    # update the information on the Duolingo courses page
    response = requests.patch(f"https://api.notion.com/v1/pages/{keychain['DUOLINGO_COURSES_PAGEID']}",headers=headers, json=page_json)
    if response.status_code != 200:
        print(f"error occured while uploaded page data: {response.text}")
    
    # update the infromation for all Duolingo courses
    for course in courses:
        notion_format = duolingo_data_notion_courses_format(keychain['DUOLINGO_COURSES_DBID'],course)        
        page_id = search_for_notion_page_by_title(headers,keychain['DUOLINGO_COURSES_DBID'],course.get('title'))
        if page_id:
            response = requests.patch(f"https://api.notion.com/v1/pages/{page_id}",headers=headers,json=notion_format)
        else:
            response = requests.post(f"https://api.notion.com/v1/pages",headers=headers,json=notion_format)
        print(f"{response.status_code} : {response.json().get('message')}")

    # update the information for all Duolingo streak data
    #for streak in streakData:
    #    print((streak,streakData[streak]))

# %%
