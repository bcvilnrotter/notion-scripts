import requests
import pandas as pd
import datetime as dt
from utils.basic_functions import *
from utils.notion.basic_functions import *
from utils.notion.database_functions import *
from utils.notion.property_formatting import *

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

def get_duolingo_calendar_skills_words(headers,dbid):
    response = get_records_from_notion_database(headers,dbid)
    response.raise_for_status()
    iterator = response.json().get('results')
    print(iterator)
    words = list({
        item.get('name') 
        for record in iterator
        for item in record
                    .get('properties')
                    .get('words')
                    .get('multi_select') 
        if record
           .get('properties')
           .get('words')
           .get('multi_select') != []})

    processed = []
    for record in iterator:
        ms = (
            record.get('properties',{})
                .get('words',{})
                .get('multi_select',[])
        )
        names = [item.get('name') for item in ms]
        processed.append((record,names))
    
    return {
        word: [rec.get('id') for rec, names in processed if word in names]
        for word in words
    }

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

def get_data_for_duolingo_dictionary_page(dbid,word,page_ids):
    head = get_new_empty_notion_page(dbid,word)
    head['properties']['🗓️ Duolingo Calendar Skills'] = format_notion_multi_relation(
        page_ids)
    return head

def update_duolingo_dictionary_database():
    key_list = [
        'NOTION_TOKEN',
        'DUOLINGO_CALENDAR_SKILLS_DBID_INNER',
        'NOTION_UNIVERSAL_DICTIONARY_DBID_INNER'
    ]
    keychains = get_keychain(key_list)
    headers = get_notion_header(keychains)
    words = get_duolingo_calendar_skills_words(
        headers,keychains['DUOLINGO_CALENDAR_SKILLS_DBID_INNER'])
    print(words)

    for word in words.keys():
        if not search_for_notion_page_by_title(
            headers,keychains['NOTION_UNIVERSAL_DICTIONARY_DBID_INNER'],word):
            response = new_entry_to_notion_database(
                headers,
                get_data_for_duolingo_dictionary_page(
                    keychains['NOTION_UNIVERSAL_DICTIONARY_DBID_INNER'],
                    word,
                    words[word]
                )
            )
            print(response.text)

# %%
def get_page_ids_from_universal_dictionary_db(headers,dbid,words):
    page_ids = []
    for word in words:
        page_id = search_for_notion_page_by_title(headers,dbid,word)
        if page_id:
            page_ids.append(page_id)
        else:
            new_entry_to_notion_database(headers,get_new_empty_notion_page(dbid,word))
            page_ids.append(search_for_notion_page_by_title(headers,dbid,word))
    return page_ids

# %%
def upload_duolingo_data_to_notion(dry_run=False):
    key_list=[
        'NOTION_TOKEN',
        'DUOLINGO_USER',
        'DUOLINGO_PROFILE_USER',
        'DUOLINGO_COURSES_PAGEID',
        'DUOLINGO_CALENDAR_SKILLS_DBID',
        'DUOLINGO_COURSES_DBID',
        'DUOLINGO_COOKIE',
        'NOTION_UNIVERSAL_DICTIONARY_DBID_INNER'
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
        
        # if record has words to be added to relation property, add them before uploading
        if calendar.get('words') is not np.nan:
            words_list = calendar.get('words')
            page_ids = get_page_ids_from_universal_dictionary_db(
                headers,keychain['NOTION_UNIVERSAL_DICTIONARY_DBID_INNER'],words_list)
            notion_format['properties']['Universal Dictionary'] = format_notion_multi_relation(page_ids)

        print('page formatted to notion.')
        
        if dry_run:
            print('dry run, not uploading.')
            continue        
        
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
