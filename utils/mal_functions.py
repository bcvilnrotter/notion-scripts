import pathlib, sys, json
from datetime import datetime as dt
from utils.basic_functions import *
from utils.notion.basic_functions import *
from utils.notion.database_functions import *
from utils.notion.property_formatting import *

def pull_todays_records_useranimelist_from_mal(mal_user,headers,date):
    
    url = f'https://api.myanimelist.net/v2/users/{mal_user}/animelist?fields=list_status&limit=100'

    records = []
    while True:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            date_data = [
                n for n in response.json()['data'] 
                if date in n['list_status']['updated_at']
            ]
            records.extend(date_data)

            if 'next' in response.json()['paging']:
                url = response.json()['paging']['next']

            else:
                break
        except Exception as e:
            print(f"[!]: {e}")
            break
    print(f"[+]: Found {len(records)} records for {mal_user} updated on {date}")
    return records

def build_notion_mal_entries_new_page(mal_record,keychain):
    update_data = {
        "parent": {
            "database_id": keychain['NOTION_MAL_ENTRIES_DBID'],
        },
        "cover": {
            "type": "external",
            "external": {
                "url": mal_record['node']['main_picture']['medium']
            }
        }
    }

    return update_data

def build_notion_mal_record(mal_record,keychain,headers,date,dry_run=False):
    update_data = {
        "parent": {
            "database_id": keychain['NOTION_MAL_RECORDS_DBID'],
        },
        "properties": {
            'Name': {
                'title': [{
                        'text': {
                            'content': f"{mal_record['node']['title']}_{date}"
                        }
                }]
            }
        }
    }

    if mal_record['node']['id']:
        update_data['properties']['node.id'] = format_notion_text(
            mal_record['node']['id'])
    if mal_record['node']['title']:
        update_data['properties']['node.title'] = format_notion_text(
            mal_record['node']['title'])
    if mal_record['list_status']['status']:
        update_data['properties']['list_status.status'] = format_notion_select(
            mal_record['list_status']['status'])
    if mal_record['list_status']['score']:
        update_data['properties']['list_status.score'] = format_notion_number(
            mal_record['list_status']['score'])
    if mal_record['list_status']['num_episodes_watched']:
        update_data['properties']['list_status.num_episodes_watched'] = format_notion_number(
            mal_record['list_status']['num_episodes_watched'])
    if mal_record['list_status']['updated_at']:
        update_data['properties']['list_status.updated_at'] = format_notion_date_outer(
            format_notion_date(
                mal_record['list_status']['updated_at'],
                string_pattern='%Y-%m-%d'))
    if mal_record['list_status']['is_rewatching'] is not None:
        update_data['properties']['list_status.is_rewatching'] = format_notion_checkbox(
            mal_record['list_status']['is_rewatching'])

    entries_page_id = search_for_notion_page_by_title(
        dbid=keychain['NOTION_MAL_ENTRIES_DBID'],
        headers=headers,
        title=mal_record['node']['title'],)

    if entries_page_id:
        update_data['properties'][
            'MAL Entries'] = format_notion_single_relation(entries_page_id)
    else:
        if dry_run:
            print(f"[DRY RUN]: Would create new page in MAL Entries database for {mal_record['node']['title']}")
        else:
            update_data['properties'][
                'MAL Entries'] = format_notion_single_relation(
                    new_entry_to_notion_database(
                        headers,
                        build_notion_mal_entries_new_page(mal_record,keychain)
                    )
                )

    return update_data

def upload_mal_to_notion(
        dry_run=False,
        mal_user='the_Fizzgig',
        mal_client_id='MAL_CLIENT_ID',
        notion_mal_entries_dbid='NOTION_MAL_ENTRIES_DBID',
        notion_mal_records_dbid='NOTION_MAL_RECORDS_DBID',
        date='today'
    ):
    if date == 'today':
        date = dt.now().strftime('%Y-%m-%d')

    keychain = get_keychain([
        'NOTION_TOKEN',
        notion_mal_entries_dbid,
        notion_mal_records_dbid,
        mal_client_id])

    daily_useranimelist_records = pull_todays_records_useranimelist_from_mal(
        mal_user=mal_user,
        headers={'X-MAL-CLIENT-ID': keychain[mal_client_id]},
        date=date
    )

    if len(daily_useranimelist_records) == 0:
        print(f"[!]: No records found for {mal_user} updated on {date}. Exiting.")
        return

    headers = get_notion_header_scalable(
        notion_token=keychain['NOTION_TOKEN'])

    for record in daily_useranimelist_records:
        notion_record = build_notion_mal_record(
            record,keychain,headers,date,dry_run=dry_run)
        if dry_run:
            print(f"[DRY RUN]: {notion_record}")