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

def get_page_from_pages_by_perigon_id(pages,perigon_id):
    results = [
        page 
        for page in pages
        if page.get('properties')
        if page.get('properties').get(
            'Perigon.id').get(
                'rich_text')[0].get(
                    'text').get('content') == perigon_id
    ]
    return results[0] if results else None

def format_perigon_story_record(
        perigon_record,stories_dbid,
        stories_pages,institutions_pages,perigon_pid):
    
    n_props = perigon_record
    n_pid = n_props.get('id')
    
    p_page = [
        i 
        for i in stories_pages 
        if get_perigon_id_from_notion_page(i) == n_pid
    ]
    p_props = p_page[0].get('properties') if p_page else {}

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

    """    
    if p_props.get('Name'):
        if p_props.get('Name').get('title')[0].get('text').get(
            'content') != n_props.get('name'):
            payload['properties']['Name'] = {
                "title": [{
                    "text": {
                        "content": perigon_record.get('name')
                    }
                }]
            }
    """

    if p_props.get('perigon.createdAt'):
        if n_props.get('createdAt') != p_props.get(
            'perigon.createdAt').get('date').get('start'):
            payload['properties'][
                'perigon.createdAt'] = format_notion_date(
                n_props.get('createdAt'))
    elif n_props.get('createdAt'):
        payload['properties'][
            'perigon.createdAt'] = format_notion_date(
            n_props.get('createdAt'))

    if p_props.get('perigon.publishedAt'):
        if n_props.get('publishedAt') != p_props.get(
            'perigon.publishedAt').get('date').get('start'):
            payload['properties'][
                'perigon.publishedAt'] = format_notion_date(
                n_props.get('publishedAt'))
    elif n_props.get('publishedAt'):
        payload['properties'][
            'perigon.publishedAt'] = format_notion_date(
            n_props.get('publishedAt'))

    if p_props.get('perigon.initializedAt'):
        if n_props.get('initializedAt') != p_props.get(
            'perigon.initializedAt').get('date').get('start'):
            payload['properties'][
                'perigon.initializedAt'] = format_notion_date(
                n_props.get('initializedAt'))
    elif n_props.get('initializedAt'):
        payload['properties'][
            'perigon.initializedAt'] = format_notion_date(
            n_props.get('initializedAt'))
    
    if p_props.get('Perigon.id'):
        if n_props.get('id') != p_props.get(
            'Perigon.id').get('rich_text')[0].get(
                'text').get('content'):
            payload['properties'][
                'Perigon.id'] = format_notion_text(
                    n_props.get('id'))
    elif n_props.get('id'):
        payload['properties'][
            'Perigon.id'] = format_notion_text(
                n_props.get('id'))
    
    """ TODO: need way to limit text length to <2000 characters
    if p_props.get('perigon.summary'):
        if n_props.get('summary') != p_props.get(
            'perigon.summary').get('rich_text')[0].get(
                'text').get('content'):
            payload['properties'][
                'perigon.summary'] = format_notion_text(
                    n_props.get('summary'))
    elif n_props.get('summary'):
        payload['properties'][
            'perigon.summary'] = format_notion_text(
                n_props.get('summary'))
    """
                
    if p_props.get('perigon.shortSummary'):
        if p_props.get('perigon.shortSummary').get('rich_text'):
            if n_props.get('shortSummary') != p_props.get(
                'perigon.shortSummary').get(
                    'rich_text')[0].get('text').get('content'):
                payload['properties'][
                    'perigon.shortSummary'] = format_notion_text(
                    n_props.get('shortSummary'))
        else:
            payload['properties'][
                'perigon.shortSummary'] = format_notion_text(
                    n_props.get('shortSummary'))
    elif n_props.get('shortSummary'):
        payload['properties'][
            'perigon.shortSummary'] = format_notion_text(
            n_props.get('shortSummary'))

    if p_props.get('sentiment.positive'):
        if n_props.get('sentiment').get(
            'positive') != p_props.get(
                'perigon.sentiment.positive').get('number'):
            payload['properties'][
                'perigon.sentiment.positive'] = format_notion_number(
                n_props.get('sentiment').get(
                    'positive'), float=True)
    elif n_props.get('sentiment').get('positive'):
        payload['properties'][
            'perigon.sentiment.positive'] = format_notion_number(
            n_props.get('sentiment').get(
                'positive'), float=True)
    
    if p_props.get('sentiment.negative'):
        if n_props.get('sentiment').get('negative') != p_props.get(
            'perigon.sentiment.negative').get('number'):
            payload['properties'][
                'perigon.sentiment.negative'] = format_notion_number(
                n_props.get('sentiment').get(
                    'negative'), float=True)
    elif n_props.get('sentiment').get('negative'):
        payload['properties'][
            'perigon.sentiment.negative'] = format_notion_number(
            n_props.get('sentiment').get(
                'negative'), float=True)
    
    if p_props.get('sentiment.neutral'):
        if n_props.get('sentiment').get('neutral') != p_props.get(
            'perigon.sentiment.neutral').get('number'):
            payload['properties'][
                'perigon.sentiment.neutral'] = format_notion_number(
                n_props.get('sentiment').get(
                    'neutral'), float=True)
    elif n_props.get('sentiment').get('neutral'):
        payload['properties'][
            'perigon.sentiment.neutral'] = format_notion_number(
            n_props.get('sentiment').get(
                'neutral'), float=True)
    
    if p_props.get('uniqueCount'):
        if n_props.get('uniqueCount') != p_props.get(
            'perigon.uniqueCount').get('number'):
            payload['properties'][
                'perigon.uniqueCount'] = format_notion_number(
                n_props.get('uniqueCount'))
    elif n_props.get('uniqueCount'):
        payload['properties'][
            'perigon.uniqueCount'] = format_notion_number(
            n_props.get('uniqueCount'))
    
    if p_props.get('reprintCount'):
        if n_props.get('reprintCount') != p_props.get(
            'perigon.reprintCount').get('number'):
            payload['properties'][
                'perigon.reprintCount'] = format_notion_number(
                n_props.get('reprintCount'))
    elif n_props.get('reprintCount'):
        payload['properties'][
            'perigon.reprintCount'] = format_notion_number(
            n_props.get('reprintCount'))
    
    if p_props.get('totalCount'):
        if n_props.get('totalCount') != p_props.get(
            'perigon.totalCount').get('number'):
            payload['properties'][
                'perigon.totalCount'] = format_notion_number(
                n_props.get('totalCount'))
    elif n_props.get('totalCount'):
        payload['properties'][
            'perigon.totalCount'] = format_notion_number(
            n_props.get('totalCount'))
    
    if n_props.get('topCountries'):
        n_countries = [i for i in n_props.get('topCountries')]
        if p_props.get('perigon.topCountries'):
            p_countries = [
                i['name'] for i in p_props.get(
                    'perigon.topCountries').get('multi_select')]
            if set(n_countries) != set(p_countries):
                payload['properties'][
                    'perigon.topCountries'] = format_notion_multi_select(
                    n_countries)
        else:
            payload['properties'][
                'perigon.topCountries'] = format_notion_multi_select(
                n_countries)
    
    if n_props.get('topTopics'):
        n_topics = [i['name'] for i in n_props.get('topTopics')]
        if p_props.get('perigon.topTopics'):
            p_topics = [i['name'] for i in p_props.get(
                'perigon.topTopics').get('multi_select')]
            if set(n_topics) != set(p_topics):
                payload['properties'][
                    'perigon.topTopics'] = format_notion_multi_select(
                    n_topics)
        else:
            payload['properties'][
                'perigon.topTopics'] = format_notion_multi_select(
                n_topics)
    
    if n_props.get('topCategories'):
        n_categories = [i['name'] for i in n_props.get(
            'topCategories')]
        if p_props.get('topCategories'):
            p_categories = [i['name'] for i in p_props.get(
                'perigon.topCategories').get('multi_select')]
            if set(n_categories) != set(p_categories):
                payload['properties'][
                    'perigon.topCategories'] = format_notion_multi_select(
                    n_categories)
        else:
            payload['properties'][
                'perigon.topCategories'] = format_notion_multi_select(
                n_categories)

    if n_props.get('topTaxonomies'):
        n_taxonomies = [i['name'] for i in n_props.get(
            'topTaxonomies')]
        if p_props.get('perigon.topTaxonomies'):
            p_taxonomies = [i['name'] for i in p_props.get(
                'perigon.topTaxonomies').get('multi_select')]
            if set(n_taxonomies) != set(p_taxonomies):
                payload['properties'][
                    'perigon.topTaxonomies'] = format_notion_multi_select(
                    n_taxonomies)
        else:
            payload['properties'][
                'perigon.topTaxonomies'] = format_notion_multi_select(
                n_taxonomies)

    if n_props.get('topPeople'):
        n_people = [i['name'] for i in n_props.get('topPeople')]
        if p_props.get('perigon.topPeople'):
            p_people = [i['name'] for i in p_props.get('perigon.topPeople').get(
                'multi_select')]
            if set(n_people) != set(p_people):
                payload['properties'][
                    'perigon.topPeople'] = format_notion_multi_select(
                    n_people)
        else:
            payload['properties'][
                'perigon.topPeople'] = format_notion_multi_select(
                n_people)
    
    if n_props.get('topCompanies'):
        list_of_companies_ids = [
            i['id'] for i in n_props.get('topCompanies')]
        n_companies = [
            page_id
            for company_id in list_of_companies_ids
            for page_id in get_page_ids_from_institutions_pages(
                institutions_pages,company_id)
            if page_id
        ]
        if p_props.get('perigon.topCompanies'):
            p_companies = [i['id'] for i in p_props.get(
                'perigon.topCompanies').get('relation')]
            if set(n_companies) != set(p_companies):
                payload['properties'][
                    'perigon.topCompanies'] = format_notion_multi_relation(
                    n_companies)
        else:
            payload['properties'][
                'perigon.topCompanies'] = format_notion_multi_relation(
                n_companies)
    
    payload['properties']['Enriched By'] = format_notion_single_relation(
        perigon_pid)
    
    return {k:v for k,v in payload.items() if v is not None}

def get_stories_pages(headers,stories_dbid):
    return get_records_from_notion_database(
        headers,stories_dbid,paginated=True
    )

def update_record_stories(
        institutions_pages,stories_pages,perigon_pages,
        stories_dbid,perigon_pid):

    return [
        [
            get_notion_page_id(
                get_page_from_pages_by_perigon_id(
                    stories_pages,
                    perigon_page.get('id')
                )
            ),
            format_perigon_story_record(
                perigon_record=perigon_page,
                stories_dbid=stories_dbid,
                stories_pages=stories_pages,
                institutions_pages=institutions_pages,
                perigon_pid=perigon_pid
            )
        ]
        for perigon_page in perigon_pages
    ]