#%%
import requests,os,json,csv
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

os.environ["KAGGLE_USERNAME"] = get_secret('KAGGLE_USERNAME')
os.environ["KAGGLE_KEY"] = get_secret('KAGGLE_API')

from kaggle import api

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

def get_notion_header(key_chain=None,notion_token=None):
    if notion_token and key_chain is None:
        token = notion_token
    elif key_chain and notion_token is None:
        token = key_chain.get('NOTION_TOKEN')
    else:
        raise ValueError("Either key_chain or notion_token must be provided.")
    return {
        'Authorization': f"Bearer {token}",
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
def format_2week_playtime_to_notion_data(key_chain,game_data):
    today = datetime.utcnow().strftime('%Y-%m-%d')
    two_weeks_ago = (datetime.utcnow() - timedelta(days=14)).strftime('%Y-%m-%d')
    if game_data['name']:
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
            "parent": {"database_id": key_chain['NOTION_RAW_PLAYTIME_DBID']},
            "properties": properties
        }
    else:
        return None

#%%
def format_notion_number(number):
    return {"number": number}

def format_notion_text(text):
    return {"rich_text": [{"text": {"content": text}}]}

def format_notion_date(start, end=None, patterns=None):
    if patterns:
        for pattern in patterns:
            try:
                start = datetime.strptime(start, pattern).strftime('%Y-%m-%d')
                if end: 
                    end = datetime.strptime(end, pattern).strftime('%Y-%m-%d')
            except:
                continue
    return {"date": {"start": start, "end": end}}

def format_notion_single_relation(page_id):
    return {"relation": [{"id": page_id}]}

def format_notion_multi_relation(page_ids):
    return {"relation": [{"id": page_id} for page_id in page_ids]}

def format_notion_multi_select(selections):
    return {"multi_select": [{"name": selection} for selection in [v.replace(',','') for v in selections]]}

def format_notion_checkbox(checked):
    return {"checkbox": checked}

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
    if response.status_code == 200 and response.json()['results'] != []:
        return response.json()["results"][0]["id"]
    else:
        return False

def update_entry_to_notion_database(headers,data,page_id):
    response = requests.patch(f'https://api.notion.com/v1/pages/{page_id}',headers=headers, data=json.dumps(data))
    return response

def new_entry_to_notion_database(headers,data):
    response = requests.post('https://api.notion.com/v1/pages',headers=headers, data=json.dumps(data))
    return response

def adjust_notion_video_game_stat_data(key_chain,headers,data):
    # initialize data from JSON data
    page_id = data.get('id')
    title = data.get('properties').get('Title').get('title')[0].get('text').get('content')
    appid = data.get('properties').get('AppId').get('rich_text')[0].get('text').get('content')
    
    # get game data
    game_data_response = requests.get(f"https://store.steampowered.com/api/appdetails?appids={appid}")
    
    # search to see if a video game stats page exists
    video_game_stats_page = search_for_notion_page_by_title(headers,key_chain['NOTION_VIDEO_GAME_STATS_DBID'],title)
    print(video_game_stats_page)

    # get a list of the relation properties based on whether a page exists
    if video_game_stats_page:
        video_game_page = requests.get(f"https://api.notion.com/v1/pages/{video_game_stats_page}",headers=headers).json()
        relation_list = video_game_page.get('properties').get('🎳 Raw Playtime').get('relation')
        relation_list.append({"id": page_id})
    else:
        relation_list = [{"id": page_id}]

    # format the data to update the notion page
    update_data = {
        "parent": {
            "database_id": key_chain['NOTION_VIDEO_GAME_STATS_DBID']
        },
        "cover": {
            "type": "external",
            "external": {
                "url": get_banner_url_from_appid(appid)
            }
        },
        "properties": {
            "Name": {"title": [{"text": {"content": title}}]},
            "🎳 Raw Playtime": {"relation": relation_list},
        }
    }

    # add conditional properties
    if game_data_response.json().get(appid).get('data').get('price_overview'):
        update_data['properties']['price_overview_initial'] = format_notion_number(game_data_response.json().get(appid).get('data').get('price_overview').get('initial'))
    if game_data_response.json().get(appid).get('data').get('metacritic'):
        update_data['properties']['metacritic_score'] = format_notion_number(game_data_response.json().get(appid).get('data').get('metacritic').get('score'))
    if game_data_response.json().get(appid).get('data').get('publishers'):
        update_data['properties']['Raw Publishers'] = format_notion_multi_select(game_data_response.json().get(appid).get('data').get('publishers'))
    if game_data_response.json().get(appid).get('data').get('developers'):
        update_data['properties']['Raw Developers'] = format_notion_multi_select(game_data_response.json().get(appid).get('data').get('developers'))
    if game_data_response.json().get(appid).get('data').get('genres'):  
        update_data['properties']['Raw Genres'] = format_notion_multi_select([tag.get('description') for tag in game_data_response.json().get(appid).get('data').get('genres') if tag])
    if game_data_response.json().get(appid).get('data').get('categories'):
        update_data['properties']['Raw Categories'] = format_notion_multi_select([tag.get('description') for tag in game_data_response.json().get(appid).get('data').get('categories') if tag])
    if game_data_response.json().get(appid).get('data').get('release_date'):
        update_data['properties']['Raw Release Date'] = format_notion_date(game_data_response.json().get(appid).get('data').get('release_date').get('date'),patterns=['%b %d, %Y','%d %b, %Y'])

    print(update_data)

    # update or create a new page as needed
    if video_game_stats_page:
        update_response = update_entry_to_notion_database(headers,update_data,video_game_stats_page)
        print(f"{update_response.status_code} : {update_response.json().get('message')}: updated game page: {title}")
    else:
        new_response = new_entry_to_notion_database(headers,update_data)
        print(f"{new_response.status_code} : {new_response.json().get('message')}: made a new game page: {title}")

def adjust_notion_video_game_stat_data_outa_sync(key_chain,headers,appid,page_id):
    # get game data
    game_data_response = requests.get(f"https://store.steampowered.com/api/appdetails?appids={appid}")
    if game_data_response.json().get(appid).get('success'):
        # search to see if a video game stats page exists
        video_game_page = requests.get(f"https://api.notion.com/v1/pages/{page_id}",headers=headers).json()        
        title = video_game_page.get('properties').get('Name').get('title')[0].get('text').get('content')
        # format the data to update the notion page
        update_data = {
            "parent": {
                "database_id": key_chain['NOTION_VIDEO_GAME_STATS_DBID']
            },
            "cover": {
                "type": "external",
                "external": {
                    "url": get_banner_url_from_appid(appid)
                }
            },
            "properties": {
            "Name": {"title": [{"text": {"content": title }}]}
            }
        }

        # add conditional properties
        if game_data_response.json().get(appid).get('data').get('price_overview'):
            update_data['properties']['price_overview_initial'] = format_notion_number(game_data_response.json().get(appid).get('data').get('price_overview').get('initial'))
        if game_data_response.json().get(appid).get('data').get('metacritic'):
            update_data['properties']['metacritic_score'] = format_notion_number(game_data_response.json().get(appid).get('data').get('metacritic').get('score'))
        if game_data_response.json().get(appid).get('data').get('publishers'):
            update_data['properties']['Raw Publishers'] = format_notion_multi_select(game_data_response.json().get(appid).get('data').get('publishers'))
        if game_data_response.json().get(appid).get('data').get('developers'):
            update_data['properties']['Raw Developers'] = format_notion_multi_select(game_data_response.json().get(appid).get('data').get('developers'))
        if game_data_response.json().get(appid).get('data').get('genres'):  
            update_data['properties']['Raw Genres'] = format_notion_multi_select([tag.get('description') for tag in game_data_response.json().get(appid).get('data').get('genres') if tag])
        if game_data_response.json().get(appid).get('data').get('categories'):
            update_data['properties']['Raw Categories'] = format_notion_multi_select([tag.get('description') for tag in game_data_response.json().get(appid).get('data').get('categories') if tag])
        if game_data_response.json().get(appid).get('data').get('release_date'):
            update_data['properties']['Raw Release Date'] = format_notion_date(game_data_response.json().get(appid).get('data').get('release_date').get('date'),patterns=['%b %d, %Y','%d %b, %Y'])

        # update or create a new page as needed
        update_response = update_entry_to_notion_database(headers,update_data,page_id)
        print(f"{update_response.status_code} : {update_response.json().get('message')}: updated game page: {page_id}")
    else:
        print(f"{game_data_response}")

def upload_2week_playtime_to_notion_database():
    print('collecting keys.')
    key_chain = get_keychain(['NOTION_TOKEN','NOTION_RAW_PLAYTIME_DBID','NOTION_VIDEO_GAME_STATS_DBID'])
    print('generate header')
    headers = get_notion_header(key_chain)    
    for record in pull_data_from_steam().get('response').get('games'):
        try:
            response = requests.post('https://api.notion.com/v1/pages',headers=headers, data=json.dumps(format_2week_playtime_to_notion_data(key_chain,record)))
            if response.status_code == 200:
                print(f"successfully added {record.get('name')} to Notion Raw Playtime.")
                adjust_notion_video_game_stat_data(key_chain,headers,response.json())
            else:
                print(f"failed to add {record.get('name')} - {response.json()}")
        except Exception as e:
            print(f"Error processing: {record}: {e}")

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
def prepare_output_folder(folder="kaggle_upload"):
    os.makedirs(folder, exist_ok=True)
    return folder

def fetch_notion_database(database_id,notion_token=None):
    if not notion_token:
        notion_token = get_secret('NOTION_TOKEN')
    headers = get_notion_header(notion_token=notion_token)
    response = requests.post(f"https://api.notion.com/v1/databases/{database_id}/query", headers=headers)
    if response.status_code == 200:
        return response.json()['results']
    else:
        print(f"Error fetching database schema: {response.text}")
        return None

def fetch_notion_database_schema(database_id,notion_token=None):
    if not notion_token:
        notion_token = get_secret('NOTION_TOKEN')
    headers = get_notion_header(notion_token=notion_token)
    response = requests.get(f"https://api.notion.com/v1/databases/{database_id}", headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching database schema: {response.text}")
        return None

def extract_formula_value(f_val):
    typ = f_val.get('type')
    return f_val.get(typ,"")

def extract_nested_rollup_value(item):
    if item.get("type") == "title":
        return "".join([t['plain_text'] for t in item['title']])
    elif item.get('type') == 'rich_text':
        return "".join([t["plain_text"] for t in item['rich_text']])
    return "[item]"

def extract_rollup_value(r_val):
    typ = r_val.get('type')
    if typ == "number":
        return r_val['number']
    elif typ == "date":
        return r_val['date'].get('start','') if r_val['date'] else ''
    elif typ == "array":
        return "; ".join(
            [extract_nested_rollup_value(item) for item in r_val.get('array',[])]
        )
    return ""

def extract_notion_property_value(prop_val, prop_type):
    val = prop_val.get(prop_type)
    if not val:
        return ""

    if prop_type == 'title':
        return "".join([t["plain_text"] for t in val])
    elif prop_type == 'rich_text':
        return "".join([t["plain_text"] for t in val])
    elif prop_type == "select":
        return val.get("name", "")
    elif prop_type == "multi_select":
        return ", ".join([v["name"] for v in val])
    elif prop_type == "number":
        return val
    elif prop_type == "checkbox":
        return val
    elif prop_type == "date":
        return val.get('start', '')
    elif prop_type == "url":
        return val
    elif prop_type == "email":
        return val
    elif prop_type == "phone_number":
        return val
    elif prop_type == "formula":
        f_val = val.get('formula',{})
        return extract_formula_value(f_val)
    elif prop_type == "rollup":
        r_val = val.get('rollup',{})
        return extract_formula_value(r_val)
    else:
        return f"[{prop_type}]"

def flatten_notion_rows_with_schema(rows,schema,outfile="notion_data.csv"):
    columns = schema['properties']
    headers = list(columns.keys())

    parsed_rows = []
    for row in rows:
        props = row['properties']
        parsed_row = {}
        for column in columns:
            prop_def = columns[column]
            prop_val = props.get(column, {})
            parsed_row[column] = extract_notion_property_value(prop_val, prop_def.get('type'))
        parsed_rows.append(parsed_row)
    
    with open(outfile,'w',newline='',encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(parsed_rows)
    
    print((f"Flattened {len(parsed_rows)} rows using schema into {outfile}"))

def write_kaggle_metadata(folder,data_slug="notion-synced-dataset",title='Notion Synced Dataset'):
    metadata = {
        "title":title,
        "id":f"{get_secret('KAGGLE_USERNAME')}/{data_slug}",
        "licenses": [{"name": "CC0-1.0"}]
    }
    with open(os.path.join(folder,'dataset-metadata.json'), 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)

def upload_to_kaggle(folder="kaggle_upload"):
    try:
        api.dataset_create_new(folder,public=False)
        print("Created new Kaggle dataset.")
    except Exception as e:
        print(f"Dataset exists - updating instead: {e}")
        api.dataset_create_version(folder,version_notes="Updated from Notion")
        print("Updated existing dataset version.")