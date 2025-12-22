import requests, json
from datetime import datetime, timedelta
from utils.notion.property_formatting import *
from utils.notion.database_functions import *
from utils.basic_functions import *

def get_banner_url_from_appid(appid):
    game_url = f'https://store.steampowered.com/api/appdetails?appids={appid}'
    response = requests.get(game_url,stream=True)
    if response.json().get(appid).get('success'):
        return json.loads(response.text).get(appid).get('data').get('header_image')
    print(f'AppId data not found: {response.text}')
    return {}

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

    # get a list of the relation properties based on whether a page exists for VG stats
    if video_game_stats_page:
        video_game_page = requests.get(f"https://api.notion.com/v1/pages/{video_game_stats_page}",headers=headers).json()
        relation_list = video_game_page.get('properties').get('🎳 Raw Playtime').get('relation')
        relation_list.append({"id": page_id})
    else:
        relation_list = [{"id": page_id}]

    # check to see if the institution database has pages for developers and publishers
    developer_relations = []
    publisher_relations = []

    if game_data_response.json().get(appid).get('developers'):
        for developer in game_data_response.json().get(appid).get('data').get('developers'):
            developer_page = search_for_notion_page_by_title(headers,key_chain['NOTION_INSTITUTIONS_DBID'],developer)
            if developer_page:
                developer_relations.append({"id": developer_page})
            else:
                # create a new page for the developer
                new_dev_page = new_entry_to_notion_database(headers, {
                    "parent": {"database_id": key_chain['NOTION_INSTITUTIONS_DBID']},
                    "properties": {
                        "Name": {"title": [{"text": {"content": developer}}]}
                    }
                })
                developer_relations.append({"id": new_dev_page.json()['id']})

    if game_data_response.json().get(appid).get('publishers'):
        for publisher in game_data_response.json().get(appid).get('data').get('publishers'):
            publisher_page = search_for_notion_page_by_title(headers,key_chain['NOTION_INSTITUTIONS_DBID'],publisher)
            if publisher_page:
                publisher_relations.append({"id": publisher_page})
            else:
                # create a new page for the publisher
                new_pub_page = new_entry_to_notion_database(headers, {
                    "parent": {"database_id": key_chain['NOTION_INSTITUTIONS_DBID']},
                    "properties": {
                        "Name": {"title": [{"text": {"content": publisher}}]}
                    }
                })
                publisher_relations.append({"id": new_pub_page.json()['id']})

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
            "Developers":format_notion_multi_relation(developer_relations),
            "Publishers":format_notion_multi_relation(publisher_relations),
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

def upload_2week_playtime_to_notion_database(dry_run=False):
    print('collecting keys.')
    key_chain = get_keychain(['NOTION_TOKEN','NOTION_RAW_PLAYTIME_DBID','NOTION_VIDEO_GAME_STATS_DBID'])
    print('generate header')
    headers = get_notion_header(key_chain)    
    for record in pull_data_from_steam().get('response').get('games'):
        if dry_run:
            print(f"Would have added {record.get('name')} to Notion Raw Playtime.")
            continue
        try:
            response = requests.post('https://api.notion.com/v1/pages',headers=headers, data=json.dumps(format_2week_playtime_to_notion_data(key_chain,record)))
            if response.status_code == 200:
                print(f"successfully added {record.get('name')} to Notion Raw Playtime.")
                adjust_notion_video_game_stat_data(key_chain,headers,response.json())
            else:
                print(f"failed to add {record.get('name')} - {response.json()}")
        except Exception as e:
            print(f"Error processing: {record}: {e}")