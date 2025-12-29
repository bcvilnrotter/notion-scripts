import os
from utils.notion.property_formatting import *
from utils.notion.basic_functions import *
from utils.notion.database_functions import *
from dotenv import load_dotenv

def get_secret(secret_key):
    if not os.getenv(secret_key):
        print(f"Loading .env for {secret_key}")
        env_path = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)),'..\..','.gitignore\.env'))
        #env_path = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)),'..\..\..','.gitignore\.env'))
        load_dotenv(dotenv_path=env_path)

    value = os.getenv(secret_key)
    print("".join(['*'])*len(value)) if value else print(f"{secret_key} wasn't pulled.")
    if value is None:
        ValueError(f"Secret '{secret_key} not found.")
    return value

def get_keychain(keys):
    return {key:get_secret(key) for key in keys}
