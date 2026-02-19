import argparse
from utils.perigon_functions import enrich_institutions_perigon

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--institutions_dbid',default='NOTION_INSTITUTIONS_DBID')
parser.add_argument('--perigon_token',default='PERIGON_TOKEN')
parser.add_argument('--perigon_app_id',default='PERIGON_APP_PAGE_ID')
args = parser.parse_args()

if __name__ == "__main__":
    enrich_institutions_perigon(**vars(args))