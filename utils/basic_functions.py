#%%
import requests,os,json,math
import datetime as dt
import pandas as pd
import numpy as np
from utils.notion_property_formatting import *
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

def prop_value_is_missing(prop):
    if pd.isna(prop):
        return True
    if prop is []:
        return True
    if prop in ['null', 'NaN', None]:
        return True
    if prop == '':
        return True
    return False

def try_notion_payload(payload):
    for i in payload:
        try:
            json.dumps(i, ensure_ascii=False, allow_nan=False)
        except ValueError as e:
            return False, i, str(e)
    return True, None, None

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

def duolingo_data_notion_calendar_skills_format(dbid,data):
    head = {'parent': {'database_id': dbid},
            'properties': {
                "Name": {
                    "title":[{
                        "text":{
                            "content":dt.datetime.fromtimestamp(
                                data['datetime']/1000
                            ).strftime('%Y-%m-%dT%H:%M:%S')
                        }
                }]
            }
        }
    }
    
    if not prop_value_is_missing(data['datetime']):
        head['properties']['datetime'] = format_notion_date(
            dt.datetime.fromtimestamp(
                data['datetime']/1000
            ),is_datetime=True,string_pattern='%Y-%m-%dT%H:%M:%S')
    if not prop_value_is_missing(data['skill_id']):
        head['properties']['skill_id'] = format_notion_text(data['skill_id'])
    if not prop_value_is_missing(data['improvement']):
        head['properties']['improvement'] = format_notion_number(data['improvement'])
    if not prop_value_is_missing(data['event_type']):
        head['properties']['event_type'] = format_notion_select(data['event_type'])
    if not prop_value_is_missing(data['language_string']):
        head['properties']['language_string'] = format_notion_select(data['language_string'])
    if not prop_value_is_missing(data['dependencies_name']):
        head['properties']['dependencies_name'] = format_notion_multi_select(data['dependencies_name'])
    if not prop_value_is_missing(data['practice_recommended']):
        head['properties']['practice_recommended'] = format_notion_checkbox(data['practice_recommended'])
    if not prop_value_is_missing(data['disabled']):
        head['properties']['disabled'] = format_notion_checkbox(data['disabled'])
    if not prop_value_is_missing(data['test_count']):
        head['properties']['test_count'] = format_notion_number(data['test_count'])
    if not prop_value_is_missing(data['skill_progress']):
        head['properties']['skill_progress'] = format_notion_number(data['skill_progress']['level'])
    if not prop_value_is_missing(data['lesson']):
        head['properties']['lesson'] = format_notion_checkbox(data['lesson'])
    if not prop_value_is_missing(data['has_explanation']):
        head['properties']['has_explanation'] = format_notion_text(data['has_explanation'])
    if not prop_value_is_missing(data['url_title']):
        head['properties']['url_title'] = format_notion_text(data['url_title'])
    if not prop_value_is_missing(data['icon_color']):
        head['properties']['icon_color'] = format_notion_select(data['icon_color'])
    if not prop_value_is_missing(data['category']): 
        head['properties']['category'] = format_notion_select(data['category'])
    if not prop_value_is_missing(data['num_lessons']):
        head['properties']['num_lessons'] = format_notion_number(data['num_lessons'])
    if not prop_value_is_missing(data['strength']):
        head['properties']['strength'] = format_notion_number(data['strength'])
    if not prop_value_is_missing(data['beginner']):
        head['properties']['beginner'] = format_notion_checkbox(data['beginner'])
    if not prop_value_is_missing(data['num_levels']):
        head['properties']['num_levels'] = format_notion_number(data['num_levels'])
    if not prop_value_is_missing(data['coords_y']):
        head['properties']['coords_y'] = format_notion_number(data['coords_y'])
    if not prop_value_is_missing(data['coords_x']):
        head['properties']['coords_x'] = format_notion_number(data['coords_x'])
    if not prop_value_is_missing(data['progress_level_session_index']):
        head['properties']['progress_level_session_index'] = format_notion_number(data['progress_level_session_index'])
    if not prop_value_is_missing(data['level_sessions_finished']):
        head['properties']['level_sessions_finished'] = format_notion_number(data['level_sessions_finished'])
    if not prop_value_is_missing(data['levels_finished']):
        head['properties']['levels_finished'] = format_notion_number(data['levels_finished'])
    if not prop_value_is_missing(data['test']): 
        head['properties']['test'] = format_notion_checkbox(data['test'])
    if not prop_value_is_missing(data['lesson_number']):
        head['properties']['lesson_number'] = format_notion_number(data['lesson_number'])
    if not prop_value_is_missing(data['learned']):
        head['properties']['learned'] = format_notion_checkbox(data['learned'])
    if not prop_value_is_missing(data['num_translation_nodes']):
        head['properties']['num_translation_nodes'] = format_notion_number(data['num_translation_nodes'])
    if not prop_value_is_missing(data['achievements']):
        head['properties']['achievements'] = format_notion_multi_select(data['achievements'])
    if not prop_value_is_missing(data['description']):
        head['properties']['description'] = format_notion_text(data['description'])
    if not prop_value_is_missing(data['index']):
        head['properties']['index'] = format_notion_number(data['index'])
    if not prop_value_is_missing(data['bonus']):
        head['properties']['bonus'] = format_notion_checkbox(data['bonus'])
    if not prop_value_is_missing(data['locked']):
        head['properties']['locked'] = format_notion_checkbox(data['locked'])
    if not prop_value_is_missing(data['explanation']):
        head['properties']['explanation'] = format_notion_text(data['explanation'])
    if not prop_value_is_missing(data['num_lexemes']):
        head['properties']['num_lexemes'] = format_notion_number(data['num_lexemes'])
    if not prop_value_is_missing(data['num_missing']):
        head['properties']['num_missing'] = format_notion_number(data['num_missing'])
    if not prop_value_is_missing(data['dependencies']):
        head['properties']['dependencies'] = format_notion_multi_select(data['dependencies'])
    if not prop_value_is_missing(data['known_lexemes']):
        head['properties']['known_lexemes'] = format_notion_multi_select(data['known_lexemes'])
    if not prop_value_is_missing(data['words']):
        head['properties']['words'] = format_notion_multi_select(data['words'])
    if not prop_value_is_missing(data['num_sessions_for_level']):
        head['properties']['num_sessions_for_level'] = format_notion_number(data['num_sessions_for_level'])
    if not prop_value_is_missing(data['path']):
        head['properties']['path'] = format_notion_multi_select(data['path'])
    if not prop_value_is_missing(data['strength_no_disabled_no_character']):
        head['properties']['strength_no_disabled_no_character'] = format_notion_number(data['strength_no_disabled_no_character'])
    if not prop_value_is_missing(data['strength_no_disabled']):
        head['properties']['strength_no_disabled'] = format_notion_number(data['strength_no_disabled'])
    if not prop_value_is_missing(data['short']):
        head['properties']['short'] = format_notion_text(data['short'])
    if not prop_value_is_missing(data['grammar']):
        head['properties']['grammar'] = format_notion_checkbox(data['grammar'])
    if not prop_value_is_missing(data['name']):
        head['properties']['name'] = format_notion_text(data['name'])
    if not prop_value_is_missing(data['language']):
        head['properties']['language'] = format_notion_select(data['language'])
    if not prop_value_is_missing(data['is_new_grammar']):
        head['properties']['is_new_grammar'] = format_notion_checkbox(data['is_new_grammar'])
    if not prop_value_is_missing(data['new_index']):
        head['properties']['new_index'] = format_notion_number(data['new_index'])
    if not prop_value_is_missing(data['progress_percent']):
        head['properties']['progress_percent'] = format_notion_number(data['progress_percent'])
    if not prop_value_is_missing(data['mastered']):
        head['properties']['mastered'] = format_notion_checkbox(data['mastered'])
    return head

# %%
def upload_duolingo_data_to_notion():
    key_list=[
        'NOTION_TOKEN',
        'DUOLINGO_USER',
        'DUOLINGO_PROFILE_USER',
        'DUOLINGO_COURSES_PAGEID',
        'DUOLINGO_CALENDAR_SKILLS_DBID',
        'DUOLINGO_COURSES_DBID',
        'DUOLINGO_COOKIE'
    ]
    keychain = get_keychain(key_list)
    headers = get_notion_header(keychain)
    page_json,courses,_ = duolingo_data_notion_format(get_duolingo_api())

    # update the information on the Duolingo courses page
    response = requests.patch(f"https://api.notion.com/v1/pages/{keychain['DUOLINGO_COURSES_PAGEID']}",
        headers=headers, json=page_json)
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

    # code for pulling daily calendar skills
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {keychain['DUOLINGO_COOKIE']}", 
        "User-Agent": "Duolingo/5.152.4 (Android 13)",
        "Accept": "application/json",
        "Content-Type": "application/json",
    })

    show = session.get(f"https://www.duolingo.com/api/1/users/show", 
        params={"username":keychain['DUOLINGO_PROFILE_USER']}, 
        headers={'Authorization': f"Bearer {keychain['DUOLINGO_COOKIE']}"})
    show.raise_for_status()

    # code for pulling the calendar per language from langauge_data
    language_data = show.json().get('language_data')
    lan_calendar = pd.DataFrame.from_dict(
        [show.json().get('language_data').get(l).get('calendar') for l in language_data][0])
    
    # code for pulling the skills database per language from language_data
    lan_skills = pd.DataFrame.from_dict(
        [show.json().get('language_data').get(l).get('skills') for l in language_data][0])
    lan_skills = lan_skills.rename(columns={'id':'skill_id'})

    # vlookup like merging of the calendar and skills dataframes using a left join to keep the calendar items primary
    skills_calendar = pd.merge(lan_calendar,lan_skills,on='skill_id',how='left')

    for calendar in skills_calendar.to_dict(orient='records'):
        notion_format = duolingo_data_notion_calendar_skills_format(
            keychain['DUOLINGO_CALENDAR_SKILLS_DBID'],calendar)
        print('page formatted to notion.')        
        check, i, err = try_notion_payload(notion_format)
        if check:
            print('notion payload is valid.')
            page_id = search_for_notion_page_by_title(
                headers,keychain['DUOLINGO_CALENDAR_SKILLS_DBID'],dt.datetime.fromtimestamp(
                    calendar['datetime']/1000).strftime('%Y-%m-%dT%H:%M:%S'))
            print('page searched in notion.')
            if page_id:
                print('updated existing page to notion.')
                response = requests.patch(f"https://api.notion.com/v1/pages/{page_id}",headers=headers,json=notion_format)
            else:
                print('posting new page to notion.')
                response = requests.post(f"https://api.notion.com/v1/pages",headers=headers,json=notion_format)
            print(response.text)
        else:
            print(f'{i}: {err}')

# %%
