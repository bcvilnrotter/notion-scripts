#%%
from utils.basic_functions import get_keychain, get_notion_header
from utils.notion.basic_functions import get_notion_header
from utils.steam_functions import get_all_page_atts, adjust_notion_video_game_stat_data_outa_sync

def main():
    print('collecting keys.')
    key_chain = get_keychain([
        'NOTION_TOKEN',
        'NOTION_VIDEO_GAME_STATS_DBID'
    ])
    print('generate header')
    headers = get_notion_header(key_chain)

    for page_id,appId in get_all_page_atts(headers,key_chain['NOTION_VIDEO_GAME_STATS_DBID']).items():
        adjust_notion_video_game_stat_data_outa_sync(key_chain,headers,appId,page_id)

if __name__ == "__main__":
    main()

