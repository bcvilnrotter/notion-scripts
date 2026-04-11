from utils.notion.database_functions import *
from utils.notion.property_formatting import *
from utils.perigon.perigon_basic_functions import *

def get_page_ids_from_institutions_pages(institutions_pages, perigon_id):
    return [
        get_notion_page_id(page)
        for page in institutions_pages
        if page.get('properties').get('Perigon.id').get(
            'rich_text') != []
        if page.get('properties').get('Perigon.id').get(
            'rich_text')[0].get('text').get('content') == perigon_id
    ]

def format_perigon_story_record(
        perigon_record,stories_dbid,institutions_pages,headers,perigon_pid):
    payload = {
        'parent': {
            'database_id': stories_dbid
        },
        "cover": {
            "type": "external",
            "external": {
                "url": perigon_record.get('imageUrl')
            }
        } if perigon_record.get('imageUrl') else None,
        'properties': {
            'Name': {
                "title": [{
                    "text": {
                        "content": perigon_record.get('name')
                    }
                }]
            }
        }
    }

    perigon_props = perigon_record
    
    if perigon_props.get('createdAt'):
        payload['properties']['perigon.createdAt'] = format_notion_date(
            perigon_props.get('createdAt'))
    
    if perigon_props.get('publishedAt'):
        payload['properties']['perigon.publishedAt'] = format_notion_date(
            perigon_props.get('publishedAt'))
        
    if perigon_props.get('initializedAt'):
        payload['properties']['perigon.initializedAt'] = format_notion_date(
            perigon_props.get('initializedAt'))
    
    if perigon_props.get('id'):
        payload['properties']['Perigon.id'] = format_notion_text(
            perigon_props.get('id'))
    
    if perigon_props.get('summary'):
        payload['properties']['perigon.summary'] = format_notion_text(
            perigon_props.get('summary'))
    
    if perigon_props.get('shortSummary'):
        payload['properties']['perigon.shortSummary'] = format_notion_text(
            perigon_props.get('shortSummary'))
    
    if perigon_props.get('sentiment', {}).get('positive'):
        payload['properties'][
            'perigon.sentiment.positive'] = format_notion_number(
            perigon_props.get('sentiment').get('positive'), float=True)
    
    if perigon_props.get('sentiment', {}).get('negative'):
        payload['properties'][
            'perigon.sentiment.negative'] = format_notion_number(
            perigon_props.get('sentiment').get('negative'), float=True)
    
    if perigon_props.get('sentiment', {}).get('neutral'):
        payload['properties'][
            'perigon.sentiment.neutral'] = format_notion_number(
            perigon_props.get('sentiment').get('neutral'), float=True)
    
    if perigon_props.get('uniqueCount'):
        payload['properties']['perigon.uniqueCount'] = format_notion_number(
            perigon_props.get('uniqueCount'))
    
    if perigon_props.get('reprintCount'):
        payload['properties']['perigon.reprintCount'] = format_notion_number(
            perigon_props.get('reprintCount'))
    
    if perigon_props.get('totalCount'):
        payload['properties']['perigon.totalCount'] = format_notion_number(
            perigon_props.get('totalCount'))
    
    if perigon_props.get('topCountries'):
        payload['properties'][
            'perigon.topCountries'] = format_notion_multi_select(
            perigon_props.get('topCountries'))
    
    if perigon_props.get('topTopics'):
        list_of_topics = [
            i['name'] for i in perigon_props.get('topTopics')]
        payload['properties'][
            'perigon.topTopics'] = format_notion_multi_select(
            list_of_topics)
    
    if perigon_props.get('topCategories'):
        list_of_categories = [
            i['name'] for i in perigon_props.get('topCategories')]
        payload['properties'][
            'perigon.topCategories'] = format_notion_multi_select(
            list_of_categories)
    
    if perigon_props.get('topTaxonomies'):
        list_of_taxonomies = [
            i['name'] for i in perigon_props.get('topTaxonomies')]
        payload['properties'][
            'perigon.topTaxonomies'] = format_notion_multi_select(
            list_of_taxonomies)
    
    if perigon_props.get('topPeople'):
        list_of_people = [
            i['name'] for i in perigon_props.get('topPeople')]
        payload['properties'][
            'perigon.topPeople'] = format_notion_multi_select(
            list_of_people)
    
    if perigon_props.get('topCompanies'):
        list_of_companies_ids = [
            i['id'] for i in perigon_props.get('topCompanies')]
        list_of_company_page_ids = [
            page_id
            for company_id in list_of_companies_ids
            for page_id in get_page_ids_from_institutions_pages(
                institutions_pages,company_id)
            if page_id
        ]
        payload['properties'][
            'perigon.topCompanies'] = format_notion_multi_relation(
            list_of_company_page_ids)
    
    payload['properties']['Enriched By'] = format_notion_single_relation(
        perigon_pid)
    
    return {k:v for k,v in payload.items() if v is not None}