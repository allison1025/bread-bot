import email_access
import img_process 
import data_process
import email.utils
import os
import logging 
import base64
import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from io import BytesIO
from email import message_from_bytes

SERVICE_ACCOUNT_FILE = '/your_path'
CLIENT_SECRET_FILE = '/your_path'
YOUR_EMAIL = 'your_email@gmail.com'  

def setup_logging():
    log_dir = '/Users/yih/Desktop/bread-bot/logs/'
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'bread-bot_{datetime.datetime.now().strftime("%Y%m%d")}.txt')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logging.info(f"\n--- script started at {datetime.datetime.now()} ---")


def main():
    setup_logging()
    
    email_senders = [
        'location1@company.com', 
        'location2@company.com',
        'location3@company.com',
        'location4@company.com'
    ]

    query = ' OR '.join(f'from:{sender}' for sender in email_senders)

    try:
        logging.info("authenticating gmail...")
        gmail_service = email_access.get_gmail_service(CLIENT_SECRET_FILE)

        logging.info("checking emails...")
        emails = email_access.search_messages(gmail_service, query)
        if not emails:
            logging.warning("no emails found")
            return

        # Get or create spreadsheet once
        sheet_id = data_process.get_or_create_spreadsheet('MB Inventory Sheet', SERVICE_ACCOUNT_FILE)
        if not sheet_id:
            logging.error("failed to get or create spreadsheet")
            return


        processed_emails = data_process.get_processed_emails(sheet_id, SERVICE_ACCOUNT_FILE)
        new_emails = [email for email in emails if email['id'] not in processed_emails]

        if not new_emails:
            logging.info("no new emails to process")
            data_process.create_analytics_sheet(sheet_id, SERVICE_ACCOUNT_FILE)
            return

        logging.info(f"found {len(new_emails)} new emails to process")

        """ #force processing
        new_emails = emails  
        logging.info(f"Processing {len(new_emails)} emails")
        """

        logging.info("getting image attachments...")
        img_attachments = email_access.get_images(gmail_service, [msg['id'] for msg in new_emails], gmail_service)
        if not img_attachments:
            logging.warning("no image attachments found")
            return

        logging.info(f"processing {len(img_attachments)} image attachments...")
        new_data = [["date", "location", "menu item", "waste count"]]  

        for idx, img_data in enumerate(img_attachments, start=1):
            logging.info(f"processing image {idx} of {len(img_attachments)}")
            try:
                if 'stream' not in img_data or not isinstance(img_data['stream'], BytesIO):
                    logging.error(f"invalid image data for image {idx}")
                    continue

                # Try image processing first
                img_stream = img_data['stream']
                img_stream.seek(0)
                res_data = img_process.process_image_from_stream(img_stream, idx)
                logging.info(f"image processing result for image {idx}: {res_data}")

                # If no image data, try email text
                if not res_data:
                    try:
                        logging.info("no image data found. processing email text:")
                        email_data = base64.urlsafe_b64decode(img_data['raw_content'].encode('ASCII'))
                        email_message = message_from_bytes(email_data)
                        
                        msg_date = email.utils.parsedate_to_datetime(email_message['date'])
                        email_date = msg_date.strftime("%m/%d/%Y")

                        # Handle multipart messages
                        email_text = None
                        if email_message.is_multipart():
                            logging.info("Processing multipart email")
                            for part in email_message.walk():
                                if part.get_content_type() == "text/plain":
                                    email_text = part.get_payload(decode=True).decode('utf-8')
                                    logging.info("found text content")
                                    break
                        else:
                            email_text = email_message.get_payload(decode=True).decode('utf-8')
                            logging.info("found single part email content")

                        if email_text:
                            res_data = img_process.parse_text(email_text, email_date)
                            logging.info(f"email text processing result: {res_data}")
                        else:
                            logging.warning("no text content found in email")

                    except Exception as e:
                        logging.error(f"error processing email text: {str(e)}", exc_info=True)
                        continue

                if res_data:  
                    location = img_data.get('location', 'Unknown')
                    logging.info(f"adding data for location: {location}")
                    for row in res_data:
                        new_data.append([row[0], location] + row[1:])
                        logging.info(f"added row: {[row[0], location] + row[1:]}")
                else: 
                    logging.warning(f"no data extracted from image {idx} or its email")

            except Exception as e:
                logging.error(f"error processing image {idx}: {str(e)}", exc_info=True)

        if len(new_data) > 1:  
            logging.info(f"adding {len(new_data)-1} new rows to spreadsheet...")
            if data_process.update_google_sheets(sheet_id, new_data, SERVICE_ACCOUNT_FILE):
                logging.info("successfully added new data to spreadsheet")
                
                #update processed_emails list
                processed_emails.update(email['id'] for email in new_emails)
                data_process.update_processed_emails(sheet_id, processed_emails, SERVICE_ACCOUNT_FILE)

                # create analytics tab
                data_process.create_analytics_sheet(sheet_id, SERVICE_ACCOUNT_FILE)

                # share sheet
                data_process.share_google_sheet(sheet_id, YOUR_EMAIL, SERVICE_ACCOUNT_FILE)
            else:
                logging.error("failed to add new data to spreadsheet")
        else:
            logging.warning("no new data to add to spreadsheet")

    except Exception as e:
        logging.error(f"error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()