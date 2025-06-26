import os
from dotenv import load_dotenv
from google.oauth2 import service_account
import json

load_dotenv()


def get_google_credentials():
    creds_json = json.loads(os.getenv('GOOGLE_APPLICATION_CREDENTIALS_NEW'))
    return service_account.Credentials.from_service_account_info(creds_json)


def get_google_sheets_credentials():
    creds_json = json.loads(os.getenv('GOOGLE_APPLICATION_CREDENTIALS_NEW'))

    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive.file'
    ]

    return service_account.Credentials.from_service_account_info(
        creds_json,
        scopes=SCOPES
    )


def get_perplexity_credentials():
    return os.getenv('PERPLEXITY_API_KEY')


def get_openai_credentials():
    return os.getenv('CHATGPT_API_KEY')
