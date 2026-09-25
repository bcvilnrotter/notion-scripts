import requests, json, time
from datetime import datetime, timedelta
from utils.notion.property_formatting import *
from utils.notion.database_functions import *
from utils.basic_functions import *
​
STEAM_STORE_URL = 'https://store.steampowered.com/api/appdetails'
STEAM_STORE_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
    ),
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
}
​
# appid -> inner 'data' dict (or None when Steam refused / had no data).
# Keeps us from hitting the store API twice for the same game in one run.
_APPDETAILS_CACHE = {}
​
def check_video_game_page_exists(response,appid):
    """True only when Steam actually returned usable data for `appid`.
​
    Steam answers throttled / blocked callers with a 200 and an empty or
    nulled body ({} or {"<appid>": null}), which used to blow up here with
    AttributeError: 'NoneType' object has no attribute 'get'.
    """
    if response is None or getattr(response,'status_code',None) != 200:
        return False
    try:
        data = response.json()
    except ValueError:
        return False
    entry = (data or {}).get(str(appid))
    return bool(entry and entry.get('success'))
​
def fetch_appdetails(appid,retries=3,backoff=(2,5,15)):
    """Return the inner 'data' dict from the Steam store API, or None.
​
    Caches per appid, retries empty/blocked responses with backoff, and logs
    the actual status + body snippet so failures are diagnosable from CI logs.
    """
    appid = str(appid)
    if appid in _APPDETAILS_CACHE:
        return _APPDETAILS_CACHE[appid]
​
    result = None
    for attempt in range(retries):
        response = None
        try:
            response = requests.get(
                STEAM_STORE_URL,
                params={'appids':appid,'cc':'us','l':'english'},
                headers=STEAM_STORE_HEADERS,
                timeout=30)
        except requests.exceptions.RequestException as err:
            print(f' ... appdetails request error for {appid}: {err}')
​
        if check_video_game_page_exists(response,appid):
            result = response.json().get(appid).get('data')
            break
​
        detail = 'no response' if response is None else (
            f'status={response.status_code} body={response.text[:200]!r}')
        print(f' ... appdetails attempt {attempt+1}/{retries} failed '
              f'for {appid}: {detail}')
        if attempt < retries - 1:
            time.sleep(backoff[min(attempt,len(backoff)-1)])
​
    _APPDETAILS_CACHE[appid] = result
    return result
​
def get_all_page_atts(headers,database_id):
    response = get_notion_database_info(headers,database_id)
​
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
​
def get_banner_url_from_appid(appid):
    game_data = fetch_appdetails(appid)
    if game_data:
        return game_data.get('header_image')
    print(f'AppId data not found: {appid}')
    return None
​
def pull_data_from_steam():
    steam_url = f"https://api.steampowered.com/IPlayerService/GetRecentlyPlayedGames/v0001/?key={get_secret('STEAM_KEY')}&steamid={get_secret('STEAM_USER')}&format=json"
    recent_playtime = requests.get(steam_url)
    return recent_playtime.json()
​
def format_2week_playtime_to_notion_data(raw_playtime_dbid,game_data):
    today = datetime.utcnow().strftime('%Y-%m-%d')
    two_weeks_ago = (datetime.utcnow() - timedelta(days=14)).strftime('%Y-%m-%d')
    if game_data['name']:
        page_name = f'{game_data["name"]}_{today}'
        properties = {
            "Name": {"title": [{"text": {"content": page_name}}]},
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
​
        return {
            "parent": {"database_id": raw_playtime_dbid},
            "properties": properties
        }
    else:
        return None
​
def format_video_game_stats_page(video_game_stats_dbid,institutions_dbid,headers,appid,title,game_data):
    """`game_data` is the inner 'data' dict returned by fetch_appdetails()."""
    properties_data = {"Name": {"title": [{"text": {"content": title}}]}}
​
    def collect_video_game_relations(dbid,headers,property):
        list_items = []
        for item in game_data.get(property) or []:
            page_id = search_for_notion_page_by_title(headers,dbid,item)
            if page_id:
                list_items.append(page_id)
            else:
                new_page = new_entry_to_notion_database(headers, {
                    "parent": {"database_id":dbid},
                    "properties": {
                        "Name": {"title": [{"text": {"content":item}}]}
                    }
                })
                if new_page is not None:
                    list_items.append(new_page.json()['id'])
        return list_items
    
    properties_data['Publishers'] = format_notion_multi_relation(
        collect_video_game_relations(institutions_dbid,headers,'publishers'))
    properties_data['Developers'] = format_notion_multi_relation(
        collect_video_game_relations(institutions_dbid,headers,'developers'))
    properties_data['app_id'] = format_notion_number(appid)
​
    if game_data.get('price_overview'):
        properties_data['price_overview_initial'] = format_notion_number(
            game_data.get('price_overview').get('initial'))
    if game_data.get('metacritic'):
        properties_data['metacritic_score'] = format_notion_number(
            game_data.get('metacritic').get('score'))
    if game_data.get('publishers'):
        properties_data['Raw Publishers'] = format_notion_multi_select(
            game_data.get('publishers'))
    if game_data.get('developers'):
        properties_data['Raw Developers'] = format_notion_multi_select(
            game_data.get('developers'))
    if game_data.get('genres'):  
        properties_data['Raw Genres'] = format_notion_multi_select(
            [tag.get('description') for tag in game_data.get('genres') if tag])
    if game_data.get('categories'):
        properties_data['Raw Categories'] = format_notion_multi_select(
            [tag.get('description') for tag in game_data.get('categories') if tag])
    if game_data.get('release_date'):
        properties_data['Raw Release Date'] = format_notion_date(
            game_data.get('release_date').get('date'),
            patterns=['%b %d, %Y','%d %b, %Y'])
​
    page_data = {
        "parent": {
            "database_id": video_game_stats_dbid
        },
        "template": {"type": "default"},
        "properties":properties_data
    }
​
    # only set a cover when we actually have a banner; an empty value makes
    # Notion reject the whole page create.
    banner_url = game_data.get('header_image')
    if banner_url:
        page_data["cover"] = {
            "type": "external",
            "external": {"url": banner_url}
        }
​
    return page_data
​
def search_for_previous_playtime(
        headers,dbid,title,date,
        prop_name="Name",
        lower_case=False):
    query_url = f"https://api.notion.com/v1/databases/{dbid}/query"
    title_formatted = title.lower() if lower_case else title
    date_end = (datetime.strptime(date,"%Y-%m-%d")+timedelta(days=1)).strftime("%Y-%m-%d")
    
    payload = {
        "filter": {
            "and": [
                {
                    "property": prop_name,
                    "title": {
                        "equals": title_formatted
                    }
                },
                {
                    "timestamp": "created_time",
                    "created_time": {
                        "on_or_after": date
                    }
                },
                                {
                    "timestamp": "created_time",
                    "created_time": {
                        "before": date_end
                    }
                }
            ]
        }
    }
​
    response = requests.post(query_url,headers=headers,json=payload)
    data = response.json()
    if response.status_code == 200 and data.get("results"):
        return data["results"][0]["id"]
    else:
        return False
​
def adjust_notion_video_game_stat_data(video_game_stats_dbid,institutions_dbid,pt_dbid,spage_id,headers,format_data):
    title = format_data.get('properties').get('Name').get('title')[0].get('text').get('content').split('_')[0]
    appid = format_data.get('properties').get('AppId').get('rich_text')[0].get('text').get('content')
​
    two_weeks_ago = (datetime.utcnow() - timedelta(days=14)).strftime('%Y-%m-%d')
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
​
    # Notion first: if the stats page already exists we never need the Steam
    # store API, which is the endpoint that gets blocked on CI runners.
    video_game_stats_page = search_for_notion_page_by_title(headers,video_game_stats_dbid,title)
    if video_game_stats_page:
        format_data['properties']['Video Game Stats'] = format_notion_single_relation(video_game_stats_page)
    else:
        game_data = fetch_appdetails(appid)
        if game_data:
            new_page = new_entry_to_notion_database(
                headers,
                format_video_game_stats_page(
                    video_game_stats_dbid,institutions_dbid,headers,appid,title,game_data))
            if new_page is not None:
                format_data['properties']['Video Game Stats'] = format_notion_single_relation(
                    new_page.json().get('id'))
        else:
            print(f' ... Video Game page data for {title} not found; '
                  f'creating playtime row without the stats relation.')
    
    pt_yesterday = search_for_previous_playtime(headers,pt_dbid,f'{title}_{yesterday}',yesterday)
    if pt_yesterday:
        format_data[
            'properties'][
                'Raw Playtime (-1 day)'] = format_notion_single_relation(pt_yesterday)
    
    pt_two_weeks_ago = search_for_previous_playtime(headers,pt_dbid,f'{title}_{two_weeks_ago}',two_weeks_ago)
    if pt_two_weeks_ago:
        format_data[
            'properties'][
                'Raw Playtime (-14 day)'] = format_notion_single_relation(pt_two_weeks_ago)
    
    format_data[
        'properties'][
            'no daily duration (-1 day)'
    ] = format_notion_checkbox(
        False) if pt_yesterday else format_notion_checkbox(True)
    
    format_data[
        'properties'][
            'has daily duration (-14 day)'
    ] = format_notion_checkbox(
        True) if pt_two_weeks_ago else format_notion_checkbox(False)
    
    format_data['properties']['🌦️ App Ecosystem'] = format_notion_single_relation(spage_id)
    
    return format_data
​
def add_image_cover_all_records():
    print('collecting keys.')
    key_chain = get_keychain(['NOTION_TOKEN','NOTION_VIDEO_GAME_STATS_DBID'])
    print('generate header')
    headers = get_notion_header(key_chain)
​
    for page_id,appId in get_all_page_atts(headers,key_chain['NOTION_VIDEO_GAME_STATS_DBID']).items():
        page_url = f'https://api.notion.com/v1/pages/{page_id}'
        banner_url = get_banner_url_from_appid(appId)
        if not banner_url:
            print(f'Skipping cover for page {page_id}: no banner for appId {appId}')
            continue
​
        print(f'Setting up adjustments to page: {page_id}')
        data = {
            'cover': {
                'type': 'external',
                'external': {'url': banner_url}
            }
        }
​
        print(f'Posting cover art to appId: {appId}')
        response = requests.patch(page_url,headers=headers, json=data)
​
        if response.status_code == 200:
            print(f'Updated cover for page: {page_id}')
        else:
            print(f'Error updated page {page_id}:', response.json())
​
def adjust_notion_video_game_stat_data_outa_sync(key_chain,headers,appid,page_id):
    # get game data
    game_data = fetch_appdetails(appid)
    if not game_data:
        print(f'No Steam store data available for appid {appid}; skipping page {page_id}.')
        return
​
    # search to see if a video game stats page exists
    video_game_page = requests.get(f"https://api.notion.com/v1/pages/{page_id}",headers=headers).json()        
    title = video_game_page.get('properties').get('Name').get('title')[0].get('text').get('content')
    # format the data to update the notion page
    update_data = {
        "parent": {
            "database_id": key_chain['NOTION_VIDEO_GAME_STATS_DBID']
        },
        "properties": {
            "Name": {"title": [{"text": {"content": title }}]}
        }
    }
​
    banner_url = game_data.get('header_image')
    if banner_url:
        update_data["cover"] = {
            "type": "external",
            "external": {"url": banner_url}
        }
​
    # add conditional properties
    if game_data.get('price_overview'):
        update_data['properties']['price_overview_initial'] = format_notion_number(
            game_data.get('price_overview').get('initial'))
    if game_data.get('metacritic'):
        update_data['properties']['metacritic_score'] = format_notion_number(
            game_data.get('metacritic').get('score'))
    if game_data.get('publishers'):
        update_data['properties']['Raw Publishers'] = format_notion_multi_select(
            game_data.get('publishers'))
    if game_data.get('developers'):
        update_data['properties']['Raw Developers'] = format_notion_multi_select(
            game_data.get('developers'))
    if game_data.get('genres'):  
        update_data['properties']['Raw Genres'] = format_notion_multi_select(
            [tag.get('description') for tag in game_data.get('genres') if tag])
    if game_data.get('categories'):
        update_data['properties']['Raw Categories'] = format_notion_multi_select(
            [tag.get('description') for tag in game_data.get('categories') if tag])
    if game_data.get('release_date'):
        update_data['properties']['Raw Release Date'] = format_notion_date(
            game_data.get('release_date').get('date'),
            patterns=['%b %d, %Y','%d %b, %Y'])
​
    # update or create a new page as needed
    update_response = update_entry_to_notion_database(headers,update_data,page_id)
    print(f"{update_response.status_code} : {update_response.json().get('message')}: updated game page: {page_id}")
​
def upload_2week_playtime_to_notion_database(
        dry_run=False,
        raw_playtime_dbid='NOTION_RAW_PLAYTIME_DBID',
        video_game_stats_dbid='NOTION_VIDEO_GAME_STATS_DBID',
        institutions_dbid='NOTION_INSTITUTIONS_DBID',
        steam_page_id='STEAM_APP_PAGE_ID'
    ):
​
    key_chain = get_keychain([
        'NOTION_TOKEN',
        raw_playtime_dbid,
        video_game_stats_dbid,
        institutions_dbid,
        steam_page_id
    ])
    headers = get_notion_header(key_chain)
​
    for record in pull_data_from_steam().get('response').get('games'):
        if dry_run:
            print(f"Would have added {record.get('name')} to Notion Raw Playtime.")
            continue
        else:
            print(f"Processing record for {record.get('name')} to Notion Raw Playtime.")
        try:
            payload = {'status':'init'}
            payload = adjust_notion_video_game_stat_data(
                key_chain[video_game_stats_dbid],
                key_chain[institutions_dbid],
                key_chain[raw_playtime_dbid],
                key_chain[steam_page_id],
                headers,
                format_2week_playtime_to_notion_data(key_chain[raw_playtime_dbid],record))
            response = new_entry_to_notion_database(headers,payload)
            response.raise_for_status()            
        except Exception as err:
            # one bad game should never abort the whole sync
            print(f"[!] Failed on {record.get('name')} ({type(err).__name__}): {err}")
            print(f"    payload: {payload}")
