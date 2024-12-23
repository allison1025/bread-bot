from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
import base64
import os
import pickle
from io import BytesIO
import logging
from email import message_from_bytes 
import time

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
MAX_DAILY_REQUESTS = 9000

def get_gmail_service(client_secret_file):
    creds = None
    token_file = 'token.pickle'
    
    if os.path.exists(token_file):
        with open(token_file, 'rb') as token:
            creds = pickle.load(token)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("refreshing token")
            creds.refresh(Request())
        else:
            print("getting new token")
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            creds = flow.run_local_server(port=0)
        
        with open(token_file, 'wb') as token:
            pickle.dump(creds, token)
    
    return build('gmail', 'v1', credentials=creds)


def search_messages(service, query):
    try:
        messages = []
        next_page_token = None
        num_requests = 0
        
        logging.info(f"searching for emails with query: {query}")
        
        while num_requests < MAX_DAILY_REQUESTS:
            try:
                result = service.users().messages().list(
                    userId='me',
                    q=query,
                    maxResults=500,
                    pageToken=next_page_token
                ).execute()
                num_requests += 1
                
                if 'messages' in result:
                    messages.extend(result['messages'])
                    logging.info(f"found {len(result['messages'])} messages, total so far: {len(messages)}")
                
                next_page_token = result.get('nextPageToken')
                if not next_page_token:
                    break
                time.sleep(0.1)
                    
            except HttpError as error:
                if error.resp.status == 429: 
                    retry_time_str = error.error_details[0]['message'].split('Retry after ')[1].rstrip('Z"]. ')
                    logging.info(f"rate limit hit - wait until {retry_time_str}")
                    break
                else:
                    raise error
                    
        logging.info(f"found {len(messages)} total messages matching the query")
        return messages
        
    except Exception as e:
        logging.error(f"error in search_messages: {str(e)}")
        return []
    
def get_images(service, msg_ids, gmail_service):
    img_data_list = []
    batch_size = 5  
    
    for i in range(0, len(msg_ids), batch_size):
        logging.info(f"processing batch {i//batch_size + 1} out of {(len(msg_ids) + batch_size - 1)//batch_size}")
        batch_ids = msg_ids[i:i + batch_size]
        
        for msg_id in batch_ids:
            try:
                msg_metadata = service.users().messages().get(
                    userId='me',
                    id=msg_id,
                    format='metadata',
                    metadataHeaders=['From']
                ).execute()
                
                from_header = next((header['value'] for header in msg_metadata['payload']['headers'] 
                                  if header['name'] == 'From'), '')
                location = 'Unknown'
                if 'location1@' in from_header.lower():
                    location = 'location1'
                elif 'location2@' in from_header.lower():
                    location = 'location2'
                elif 'location3@' in from_header.lower():
                    location = 'location3'
                elif 'location4@' in from_header.lower():
                    location = 'location4'
                
                logging.info(f"processing email from {location} with id: {msg_id}")
                
                msg = service.users().messages().get(
                    userId='me',
                    id=msg_id,
                    format='raw'
                ).execute()
                
                email_data = base64.urlsafe_b64decode(msg['raw'].encode('ASCII'))
                email_message = message_from_bytes(email_data)
                
                for part in email_message.walk():
                    if part.get_content_maintype() == 'image':
                        img_data = part.get_payload(decode=True)
                        if img_data:
                            img_stream = BytesIO(img_data)
                            img_data_list.append({
                                'stream': img_stream,
                                'filename': part.get_filename() or 'unknown',
                                'content_id': part.get('Content-ID'),
                                'email_id': msg_id,
                                'location': location,
                                'raw_content': msg['raw'] 
                            })
                
                time.sleep(0.1) 
                
            except Exception as e:
                logging.error(f"error processing for email {msg_id}: {str(e)}")
                continue
        
        time.sleep(0.5)  
        
    logging.info(f"processed {len(msg_ids)} emails, and found {len(img_data_list)} images")
    return img_data_list