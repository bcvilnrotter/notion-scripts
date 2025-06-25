#%%
import os
from utils.basic_functions import (
    upload_2week_playtime_to_notion_database,
    get_secret,
    upload_to_kaggle,
    prepare_output_folder,
    write_kaggle_metadata,
    flatten_notion_rows_with_schema,
    fetch_notion_database,
    fetch_notion_database_schema
)
#%%
if __name__ == "__main__":
    #%%
    # update 2-week playtime in Notion database
    upload_2week_playtime_to_notion_database()

    #%% pull database that was updated and post to Kaggle
    folder = prepare_output_folder()
    upload_to_kaggle(
        write_kaggle_metadata(folder) or
        (
            ats := {
                'database_id': get_secret('NOTION_VIDEO_GAME_STATS_DBID'),
                'notion_token': get_secret('NOTION_TOKEN')
            }
        ) and
        flatten_notion_rows_with_schema(
            fetch_notion_database(**ats),
            fetch_notion_database_schema(**ats),
            outfile=os.path.join(folder,'notion_video_game_stats.csv')
        ) or folder
    )
# %%
