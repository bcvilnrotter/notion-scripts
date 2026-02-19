import argparse
from utils.steam_functions import upload_2week_playtime_to_notion_database

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--dry_run',action='store_true',default=False)
parser.add_argument('--video_game_stats_dbid',default='NOTION_VIDEO_GAME_STATS_DBID')
parser.add_argument('--raw_playtime_dbid',default='NOTION_RAW_PLAYTIME_DBID')
parser.add_argument('--institutions_dbid',default='NOTION_INSTITUTIONS_DBID')
parser.add_argument('--steam_page_id',default='STEAM_APP_PAGE_ID')
args = parser.parse_args()

if __name__ == "__main__":
    upload_2week_playtime_to_notion_database(**vars(args))