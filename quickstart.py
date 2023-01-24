import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file oauth_token.json.
SCOPES = ['https://mail.google.com/']


def get_service():
    """
    Shows basic usage of the Gmail API.
    Lists the user's Gmail labels.
    """
    # The file oauth_token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.

    credentials = None
    if os.path.exists('oauth_token.json'):
        credentials = Credentials.from_authorized_user_file('oauth_token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('oauth_cred.json', SCOPES)
            credentials = flow.run_local_server()

        # Save the credentials for the next run
        with open('oauth_token.json', 'w') as token:
            token.write(credentials.to_json())

    try:
        # Call the Gmail API
        return build('gmail', 'v1', credentials=credentials, cache_discovery=False)

    except HttpError as error:
        print(f'An error occurred: {error}')

