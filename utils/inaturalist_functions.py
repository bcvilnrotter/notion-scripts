from tqdm import tqdm
from utils.basic_functions import *
from utils.notion.basic_functions import *
from utils.notion.database_functions import *
from utils.notion.property_formatting import *
import pyinaturalist as pyi
import pandas as pd
import numpy as np

def compare_jsons_single_property(primary,secondary):
    return primary if primary != secondary else None

def compare_properties(primary,secondary):
    return {
        k:v 
        for k,v in primary.items()
        if k in secondary.keys() and compare_jsons_single_property(primary[k],secondary[k]) != None
    }

def build_comparison_json(json,included_keys=None):
    if included_keys:
        return {k:dynamic_notion_formatter(v.get('type'),v) for k,v in json.items() if k in included_keys}
    return {k:dynamic_notion_formatter(v.get('type'),v) for k,v in json.items()}

def replace_none(obj):
    if isinstance(obj,dict):
        return {k:replace_none(v) for k,v in obj.items()}
    elif isinstance(obj,list):
        return [replace_none(item) for item in obj]
    elif obj is None:
        return ""
    else:
        return obj

def format_inaturalist_observation(dbid,data):
    update_data = {
        'parent': {'database_id': dbid},
        'properties': {
            "Name": {
                "title":[{
                    "text":{
                        "content":str(data['id'])
                    }
            }]
        }}
    }

    if not prop_value_is_missing(data['taxon.name']):
        update_data['properties']['taxon.name'] = format_notion_text(data['taxon.name'])
    if not prop_value_is_missing(data['taxon.preferred_common_name']):
        update_data['properties']['taxon.preferred_common_name'] = format_notion_text(data['taxon.preferred_common_name'])
    if not prop_value_is_missing(data['taxon.complete_rank']):
        update_data['properties']['taxon.complete_rank'] = format_notion_select(data['taxon.complete_rank'])
    if not prop_value_is_missing(data['taxon.iconic_taxon_name']):
        update_data['properties']['taxon.iconic_taxon_name'] = format_notion_select(data['taxon.iconic_taxon_name'])
    if not prop_value_is_missing(data['uri']) or data['uri'] != '':
        update_data['properties']['uri'] = format_notion_url(data['uri'])
    if not prop_value_is_missing(data['taxon.wikipedia_url']):
        update_data['properties']['taxon.wikipedia_url'] = format_notion_url(data['taxon.wikipedia_url'])
    if not prop_value_is_missing(data['outlinks.source']):
        update_data['properties']['outlinks.source'] = format_notion_select(data['outlinks.source'])
    if not prop_value_is_missing(data['outlinks.url']):
        update_data['properties']['outlinks.url'] = format_notion_url(data['outlinks.url'])
    if not prop_value_is_missing(data['observed_on']):
        update_data['properties']['observed_on'] = format_notion_date(data['observed_on'],is_datetime=True,string_pattern='%Y-%m-%dT%H:%M')
    if not prop_value_is_missing(data['time_zone_offset']):
        update_data['properties']['time_zone_offset'] = format_notion_select(data['time_zone_offset'])
    if not prop_value_is_missing(data['updated_at']):
        update_data['properties']['updated_at'] = format_notion_date(data['updated_at'],is_datetime=True,string_pattern='%Y-%m-%dT%H:%M')
    if not prop_value_is_missing(data['quality_grade']):
        update_data['properties']['quality_grade'] = format_notion_select(data['quality_grade'])
    if not prop_value_is_missing(data['identifications_most_agree']):
        update_data['properties']['identifications_most_agree'] = format_notion_checkbox(data['identifications_most_agree'])
    if not prop_value_is_missing(data['identifications_most_disagree']):
        update_data['properties']['identifications_most_disagree'] = format_notion_checkbox(data['identifications_most_disagree'])
    if not prop_value_is_missing(data['num_identification_agreements']):
        update_data['properties']['num_identification_agreements'] = format_notion_number(data['num_identification_agreements'])
    if not prop_value_is_missing(data['identification_disagreements_count']):
        update_data['properties']['identification_disagreements_count'] = format_notion_number(data['identification_disagreements_count'])
    if not prop_value_is_missing(data['geojson.type']):
        update_data['properties']['geojson.type'] = format_notion_select(data['geojson.type'])
    if not prop_value_is_missing(data['geojson.coordinates.lat']):
        update_data['properties']['geojson.coordinates.lat'] = format_notion_number(data['geojson.coordinates.lat'])
    if not prop_value_is_missing(data['geojson.coordinates.long']):
        update_data['properties']['geojson.coordinates.long'] = format_notion_number(data['geojson.coordinates.long'])
    if not prop_value_is_missing(data['taxon.conservation_status.status_name']):
        update_data['properties']['taxon.conservation_status.status_name'] = format_notion_select(data['taxon.conservation_status.status_name'])
    if not prop_value_is_missing(data['taxon.conservation_status.authority']):
        update_data['properties']['taxon.conservation_status.authority'] = format_notion_select(data['taxon.conservation_status.authority'])

    return update_data

def get_taxa_top_cover_image_medium_url(taxon_name):
    taxa_response = pyi.get_taxa(q=taxon_name)
    return taxa_response['results'][0]['default_photo'].get('medium_url')

def wrap_full_new_observation_page(headers,obs,tax_dbid):
    obs_taxon_name = obs['properties']['taxon.name']['rich_text'][0]['text']['content']
    taxa_page_id = search_for_notion_page_by_title(headers,tax_dbid,obs_taxon_name)
    if taxa_page_id:
        obs['properties']['iNaturalist Dictionary'] = format_notion_single_relation(taxa_page_id)
    else:
        new_taxa_page = new_entry_to_notion_database(headers,{
            "parent": {
                "database_id": tax_dbid
            },
            "cover": {
                "type": "external",
                "external": {
                    "url": get_taxa_top_cover_image_medium_url(obs_taxon_name)
                }
            },
            "properties": {
            "Name": {"title": [{"text": {"content": obs_taxon_name }}]}
            }
        })
        obs['properties']['iNaturalist Dictionary'] = format_notion_single_relation(new_taxa_page.json()['id'])
    return obs

def get_full_new_observation_page(headers,obs,obs_dbid,tax_dbid):
    obs_format = format_inaturalist_observation(obs_dbid,obs)
    obs_taxon_name = obs['properties']['taxon.name']['rich_text'][0]['text']['content']
    taxa_page_id = search_for_notion_page_by_title(headers,tax_dbid,obs_taxon_name)
    if taxa_page_id:
        obs_format['properties']['iNaturalist Dictionary'] = format_notion_single_relation(taxa_page_id)
    else:
        new_taxa_page = new_entry_to_notion_database(headers,{
            "parent": {
                "database_id": tax_dbid
            },
            "cover": {
                "type": "external",
                "external": {
                    "url": get_taxa_top_cover_image_medium_url(obs_taxon_name)
                }
            },
            "properties": {
            "Name": {"title":[{"text":{"content": obs_taxon_name}}]}
            }
        })
        obs_format['properties']['iNaturalist Dictionary'] = format_notion_single_relation(new_taxa_page.json()['id'])
    return obs_format

def update_inaturalist_db():
    key_chain = get_keychain(
        ['NOTION_TOKEN','INATURALIST_OBSERVATIONS_DBID','INATURALIST_DICTIONARY_DBID','INATURALIST_USER'])
    headers = get_notion_header_scalable(key_chain['NOTION_TOKEN'])

    observations = pyi.get_observations(user_id=key_chain['INATURALIST_USER'],page='all')
    print(f'... Collected {len(observations.get("results",[]))} records from iNaturalist API.')
    cleaned = [replace_none(r) for r in observations.get("results",[])]
    pdf = pd.json_normalize(cleaned,sep='.')
    pdf = pdf.replace([np.nan,np.inf,-np.inf],None)

    pdf['outlinks.source'] = pdf['outlinks'].apply(
        lambda x: x[0].get('source') if x != [] else np.nan)
    pdf['outlinks.url'] = pdf['outlinks'].apply(
        lambda x: x[0].get('url') if x != [] else np.nan)
    pdf[['geojson.coordinates.lat','geojson.coordinates.long']] = pd.DataFrame(
        pdf['geojson.coordinates'].apply(pd.Series),index=pdf.index)
    print(f'... Performed feature engineering transformations.')

    new_entries = [format_inaturalist_observation(key_chain['INATURALIST_OBSERVATIONS_DBID'],o) for _,o in pdf.iterrows()]
    print(f'... Created new_value json blob.')

    obs_records = request_paginated_data(
        f'https://api.notion.com/v1/databases/{key_chain["INATURALIST_OBSERVATIONS_DBID"]}'
        f'/query',headers)
    obs_records_id_first = {
        o.get('properties').get('Name').get('title')[0].get('plain_text'):o 
        for o in obs_records}

    def pull_id(item):
        return item['properties']['Name']['title'][0]['text']['content']

    prop_list =[
        'Name',
        'taxon.name',
        'taxon.preferred_common_name',
        'taxon.complete_rank',
        'taxon.iconic_taxon_name',
        'uri',
        'taxon.wikipedia_url',
        'outlinks.source',
        'outlinks.url',
        'observed_on',
        'time_zone_offset',
        'updated_at',
        'quality_grade',
        'identifications_most_agree',
        'identifications_most_disagree',
        'num_identification_agreements',
        'identification_disagreements_count',
        'geojson.type',
        'geojson.coordinates.lat',
        'geojson.coordinates.long',
        'taxon.conservation_status.status_name',
        'taxon.conservation_status.authority',       
    ]

    diff_dict = {
        pull_id(entry): (
            {'new_entry': entry} 
            if pull_id(entry) not in obs_records_id_first else
            (
                {'update': compare_properties(
                    entry.get('properties'),
                    build_comparison_json(
                        obs_records_id_first[pull_id(entry)].get('properties'),prop_list))}
                if compare_properties(entry.get('properties'),build_comparison_json(
                    obs_records_id_first[
                        pull_id(entry)].get('properties'),prop_list)) != {} else {'dup':{}}
            )    
        )
        for entry in new_entries 
    }

    filtered_diff_data = {k:v for k,v in diff_dict.items() if 'dup' not in v}
    filtered_diff_data_update = {k:v for k,v in filtered_diff_data.items() if 'update' in v}
    filtered_diff_data_new = {k:v for k,v in filtered_diff_data.items() if 'new_entry' in v}

    print(f'... Identified {len(filtered_diff_data) } records for uploading, with {len(filtered_diff_data_new)} new records, and {len(filtered_diff_data_update)} updates.')

    for _,v in tqdm(
        filtered_diff_data_new.items(),
        total=len(filtered_diff_data_new),
        desc='... Uploading new observations.'):
        data = v['new_entry']

        if 'taxon.name' in data['properties']:
            new_entry_to_notion_database(headers,wrap_full_new_observation_page(
                headers,data,key_chain['INATURALIST_DICTIONARY_DBID']))
    
    for id,v in tqdm(filtered_diff_data_update.items(),total=len(filtered_diff_data_update),desc='... Updating preexisting observations.'):
        data = v['update']
        update_entry_to_notion_database(headers,{'properties':data},search_for_notion_page_by_title(
            headers,key_chain['INATURALIST_OBSERVATIONS_DBID'],id))


"""
quality_grade Counter({'str': 1566})
taxon_geoprivacy Counter({'str': 1566})
annotations Counter({'list': 1566})
uuid Counter({'str': 1566})
id Counter({'int': 1566})
cached_votes_total Counter({'int': 1566})
identifications_most_agree Counter({'bool': 1566})
species_guess Counter({'str': 1566})
identifications_most_disagree Counter({'bool': 1566})
tags Counter({'list': 1566})
positional_accuracy Counter({'int': 1382, 'str': 184})
comments_count Counter({'int': 1566})
site_id Counter({'int': 1566})
created_time_zone Counter({'str': 1566})
license_code Counter({'str': 1566})
observed_time_zone Counter({'str': 1566})
quality_metrics Counter({'list': 1566})
public_positional_accuracy Counter({'int': 1383, 'str': 183})
reviewed_by Counter({'list': 1566})
oauth_application_id Counter({'int': 1566})
flags Counter({'list': 1566})
created_at Counter({'datetime': 1566})
description Counter({'str': 1566})
time_zone_offset Counter({'str': 1566})
project_ids_with_curator_id Counter({'list': 1566})
observed_on Counter({'datetime': 1566})
observed_on_string Counter({'str': 1566})
updated_at Counter({'datetime': 1566})
sounds Counter({'list': 1566})
place_ids Counter({'list': 1566})
captive Counter({'bool': 1566})
ident_taxon_ids Counter({'list': 1566})
outlinks Counter({'list': 1566})
faves_count Counter({'int': 1566})
ofvs Counter({'list': 1566})
num_identification_agreements Counter({'int': 1566})
identification_disagreements_count Counter({'float': 1101})
comments Counter({'list': 1566})
map_scale Counter({'str': 1566})
uri Counter({'str': 1566})
project_ids Counter({'list': 1566})
community_taxon_id Counter({'int': 898, 'str': 668})
owners_identification_from_vision Counter({'bool': 1557, 'str': 9})
identifications_count Counter({'int': 1566})
obscured Counter({'bool': 1566})
num_identification_disagreements Counter({'int': 1566})
geoprivacy Counter({'str': 1566})
location Counter({'list': 1553, 'str': 13})
votes Counter({'list': 1566})
spam Counter({'bool': 1566})
mappable Counter({'bool': 1566})
identifications_some_agree Counter({'bool': 1566})
project_ids_without_curator_id Counter({'list': 1566})
place_guess Counter({'str': 1566})
identifications Counter({'list': 1566})
project_observations Counter({'list': 1566})
observation_photos Counter({'list': 1566})
photos Counter({'list': 1566})
faves Counter({'list': 1566})
non_owner_ids Counter({'list': 1566})
observed_on_details.date Counter({'str': 1566})
observed_on_details.day Counter({'int': 1566})
observed_on_details.month Counter({'int': 1566})
observed_on_details.year Counter({'int': 1566})
observed_on_details.hour Counter({'int': 1566})
observed_on_details.week Counter({'int': 1566})
created_at_details.date Counter({'str': 1566})
created_at_details.day Counter({'int': 1566})
created_at_details.month Counter({'int': 1566})
created_at_details.year Counter({'int': 1566})
created_at_details.hour Counter({'int': 1566})
created_at_details.week Counter({'int': 1566})
taxon.is_active Counter({'bool': 1564})
taxon.ancestry Counter({'str': 1564})
taxon.min_species_ancestry Counter({'str': 1564})
taxon.endemic Counter({'bool': 1564})
taxon.iconic_taxon_id Counter({'int': 1560, 'str': 4})
taxon.min_species_taxon_id Counter({'float': 1564})
taxon.threatened Counter({'bool': 1564})
taxon.rank_level Counter({'float': 1564})
taxon.introduced Counter({'bool': 1564})
taxon.native Counter({'bool': 1564})
taxon.parent_id Counter({'int': 1560, 'str': 4})
taxon.name Counter({'str': 1564})
taxon.rank Counter({'str': 1564})
taxon.extinct Counter({'bool': 1564})
taxon.id Counter({'float': 1564})
taxon.ancestor_ids Counter({'list': 1564})
taxon.provisional Counter({'bool': 1561})
taxon.created_at Counter({'str': 1564})
taxon.default_photo.id Counter({'float': 1564})
taxon.default_photo.license_code Counter({'str': 1564})
taxon.default_photo.attribution Counter({'str': 1564})
taxon.default_photo.url Counter({'str': 1564})
taxon.default_photo.original_dimensions.height Counter({'float': 1564})
taxon.default_photo.original_dimensions.width Counter({'float': 1564})
taxon.default_photo.flags Counter({'list': 1564})
taxon.default_photo.attribution_name Counter({'str': 1564})
taxon.default_photo.square_url Counter({'str': 1564})
taxon.default_photo.medium_url Counter({'str': 1564})
taxon.taxon_changes_count Counter({'float': 1564})
taxon.taxon_schemes_count Counter({'float': 1564})
taxon.observations_count Counter({'float': 1564})
taxon.photos_locked Counter({'bool': 1564})
taxon.universal_search_rank Counter({'float': 1564})
taxon.flag_counts.resolved Counter({'float': 1564})
taxon.flag_counts.unresolved Counter({'float': 1564})
taxon.current_synonymous_taxon_ids Counter({'str': 1564})
taxon.atlas_id Counter({'str': 1262, 'int': 302})
taxon.complete_species_count Counter({'str': 1523, 'int': 41})
taxon.wikipedia_url Counter({'str': 1564})
taxon.iconic_taxon_name Counter({'str': 1560})
taxon.preferred_common_name Counter({'str': 1490})
preferences.prefers_community_taxon Counter({'str': 1566})
geojson.type Counter({'str': 1553})
geojson.coordinates Counter({'list': 1553})
user.id Counter({'int': 1566})
user.login Counter({'str': 1566})
user.spam Counter({'bool': 1566})
user.suspended Counter({'bool': 1566})
user.created_at Counter({'str': 1566})
user.site_id Counter({'int': 1566})
user.login_autocomplete Counter({'str': 1566})
user.login_exact Counter({'str': 1566})
user.name Counter({'str': 1566})
user.name_autocomplete Counter({'str': 1566})
user.orcid Counter({'str': 1566})
user.icon Counter({'str': 1566})
user.observations_count Counter({'int': 1566})
user.identifications_count Counter({'int': 1566})
user.journal_posts_count Counter({'int': 1566})
user.activity_count Counter({'int': 1566})
user.species_count Counter({'int': 1566})
user.annotated_observations_count Counter({'int': 1566})
user.universal_search_rank Counter({'int': 1566})
user.roles Counter({'list': 1566})
user.icon_url Counter({'str': 1566})
taxon.complete_rank Counter({'str': 234})
taxon.conservation_status.id Counter({'float': 28})
taxon.conservation_status.place_id Counter({'str': 28})
taxon.conservation_status.source_id Counter({'int': 23, 'str': 5})
taxon.conservation_status.user_id Counter({'str': 21, 'int': 7})
taxon.conservation_status.authority Counter({'str': 28})
taxon.conservation_status.status Counter({'str': 28})
taxon.conservation_status.status_name Counter({'str': 28})
taxon.conservation_status.geoprivacy Counter({'str': 28})
taxon.conservation_status.iucn Counter({'float': 28})
geojson Counter({'str': 13})
taxon Counter({'str': 2})
"""
