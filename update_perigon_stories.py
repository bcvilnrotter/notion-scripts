import argparse
from utils.perigon_functions import pull_stories_by_institution

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--institution_dbid',default='NOTION_INSTITUTIONS_DBID')
parser.add_argument('--stories_dbid',default='NOTION_STORIES_DBID')
parser.add_argument('--perigon_token',default='PERIGON_TOKEN')
parser.add_argument('--perigon_app_id',default='PERIGON_APP_PAGE_ID')
args = parser.parse_args()

if __name__ == "__main__":
    pull_stories_by_institution(**vars(args))