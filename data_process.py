import os
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import logging 
from collections import defaultdict

def get_processed_emails(sheet_id, credentials_file):
    try:
        creds = Credentials.from_service_account_file(credentials_file, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        service = build('sheets', 'v4', credentials=creds)
        
        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range='ProcessedEmails!A:A'
            ).execute()

            values = result.get('values', [])
            if values:
                return set(item for sublist in values for item in sublist)
            return set()
        except HttpError as e:
            if e.resp.status == 404: #nonexistent sheet
                try:
                    service.spreadsheets().batchUpdate(
                        spreadsheetId=sheet_id,
                        body={
                            "requests": [{
                                "addSheet": {
                                    "properties": {
                                        "title": "ProcessedEmails"
                                    }
                                }
                            }]
                        }
                    ).execute()
                    return set()
                except Exception as e:
                    logging.error(f"error creating ProcessedEmails sheet: {e}")
                    return set()
            else:
                logging.error(f"error getting processed emails: {e}")
                return set()
    except Exception as e:
        logging.error(f"error getting processed emails: {e}")
        return set()


def update_processed_emails(sheet_id, email_ids, credentials_file):
    try:
        creds = Credentials.from_service_account_file(credentials_file, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        service = build('sheets', 'v4', credentials=creds)
        
        unique_email_ids = list(set(email_ids))
        values = [[email_id] for email_id in email_ids]
        chunk_size = 1000  
        
        for i in range(0, len(values), chunk_size):
            chunk = values[i:i + chunk_size]
            service.spreadsheets().values().append(
                spreadsheetId=sheet_id,
                range='ProcessedEmails!A:A',
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body={'values': chunk}
            ).execute()
            
        logging.info(f"updated processed emails list with {len(unique_email_ids)} entries")
    except Exception as e:
        logging.error(f"error updating processed emails: {e}")


def get_or_create_spreadsheet(title, credentials_file):
    config_file = 'mb_inventory_config.json'
    
    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            config = json.load(f)
            sheet_id = config.get('sheet_id')
        if sheet_id:
            logging.info(f"using existing spreadsheet")
            return sheet_id
    
    creds = Credentials.from_service_account_file(credentials_file, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    service = build('sheets', 'v4', credentials=creds)

    # create spreadsheet with two sheet tabs
    try:
        spreadsheet = {
            'properties': {'title': title},
            'sheets': [
                {'properties': {'title': 'Sheet1'}},
                {'properties': {
                    'title': 'ProcessedEmails',
                    'hidden': True  
                }}
            ]
        }
        sheet = service.spreadsheets().create(body=spreadsheet, fields='spreadsheetId').execute()

        sheet_id = sheet.get('spreadsheetId')
        logging.info(f"created new spreadsheet with ID: {sheet_id}")
        
        header = [["date", "location", "menu item", "waste count"]]
        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range='Sheet1!A1',
            valueInputOption='RAW',
            body={'values': header}
        ).execute()
        
        with open(config_file, 'w') as f:
            json.dump({'sheet_id': sheet_id}, f)
        
        return sheet_id
    except Exception as e:
        logging.error(f"error creating spreadsheet: {e}")
        return None

def update_google_sheets(sheet_id, new_data, credentials_file):
    try:
        creds = Credentials.from_service_account_file(credentials_file, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        service = build('sheets', 'v4', credentials=creds)

        data_to_append = new_data[1:] if new_data and len(new_data) > 1 else []
        
        if data_to_append:
            formatted_data = []
            for row in data_to_append:
                formatted_row = [
                    f'=DATE({row[0].split("/")[2]}, {row[0].split("/")[0]}, {row[0].split("/")[1]})',  # Date formula
                    row[1] if len(row) > 1 else '',  # location
                    row[2] if len(row) > 2 else '',  # menu item
                    row[3] if len(row) > 3 else ''   # waste count
                ]
                formatted_data.append(formatted_row)

            body = {'values': formatted_data}
            result = service.spreadsheets().values().append(
                spreadsheetId=sheet_id,
                range='Sheet1!A1',
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            
            updated_range = result.get('updates').get('updatedRange')
            range_parts = updated_range.split('!')
            if len(range_parts) > 1:              
                sheet_tab_id = get_sheet_id_by_name(service, sheet_id, 'Sheet1')
                if sheet_tab_id is not None:
                    format_requests = {
                        'requests': [
                            {
                                'repeatCell': {
                                    'range': {
                                        'sheetId': sheet_tab_id,  
                                        'startColumnIndex': 0,
                                        'endColumnIndex': 1,
                                        'startRowIndex': 1  
                                    },
                                    'cell': {
                                        'userEnteredFormat': {
                                            'numberFormat': {
                                                'type': 'DATE',
                                                'pattern': 'MM/dd/yyyy'
                                            }
                                        }
                                    },
                                    'fields': 'userEnteredFormat.numberFormat'
                                }
                            },
                            {
                                'repeatCell': {
                                    'range': {
                                        'sheetId': sheet_tab_id, 
                                        'startColumnIndex': 3,  
                                        'endColumnIndex': 4,
                                        'startRowIndex': 1  
                                    },
                                    'cell': {
                                        'userEnteredFormat': {
                                            'numberFormat': {
                                                'type': 'NUMBER',
                                                'pattern': '#,##0'
                                            }
                                        }
                                    },
                                    'fields': 'userEnteredFormat.numberFormat'
                                }
                            }
                        ]
                    }
                    
                    service.spreadsheets().batchUpdate(
                        spreadsheetId = sheet_id,  
                        body = format_requests
                    ).execute()
            
            logging.info(f"appended {len(formatted_data)} new rows to the sheet")
            return True
        else:
            logging.info("no new data to append")
            return False
            
    except Exception as e:
        logging.error(f"error updating sheet: {e}")
        return False
    
def share_google_sheet(spreadsheet_id, email, credentials_file):
    creds = Credentials.from_service_account_file(credentials_file, scopes=['https://www.googleapis.com/auth/drive.file'])
    service = build('drive', 'v3', credentials=creds)
    
    try:
        permissions = service.permissions().list(fileId=spreadsheet_id).execute()
        logging.info(f"current permissions: {permissions}")

        request_body = {
            'role': 'writer',
            'type': 'user',
            'emailAddress': email
        }
        result = service.permissions().create(
            fileId=spreadsheet_id, 
            body=request_body,
            fields='id'
        ).execute()
        logging.info(f"sharing result: {result}")

        # Verify the new permissions
        new_permissions = service.permissions().list(fileId=spreadsheet_id).execute()
        logging.info(f"updated permissions: {new_permissions}")

        logging.info(f"spreadsheet shared with {email}")    
        file = service.files().get(fileId=spreadsheet_id, fields='webViewLink').execute()
        logging.info(f"spreadsheet can be viewed at: {file.get('webViewLink')}")

    except HttpError as error:
        logging.error(f"http error: {error}")
        logging.error(f"http error details: {error.content}")
    except Exception as e:
        logging.error(f"unexpected error in sharing: {e}")


def create_analytics_sheet(sheet_id, credentials_file):
    try:
        creds = Credentials.from_service_account_file(
            credentials_file,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        service = build("sheets", "v4", credentials=creds)

        response = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        analytics_sheet_id = None
        chart_data_sheet_id = None
        
        for sheet in response["sheets"]:
            if sheet["properties"]["title"] == "Analytics":
                analytics_sheet_id = sheet["properties"]["sheetId"]
            elif sheet["properties"]["title"] == "ChartData":
                chart_data_sheet_id = sheet["properties"]["sheetId"]

        requests = []
        if not analytics_sheet_id:
            requests.append({
                "addSheet": {
                    "properties": {"title": "Analytics"}
                }
            })
        if not chart_data_sheet_id:
            requests.append({
                "addSheet": {
                    "properties": {
                        "title": "ChartData",
                        "hidden": True
                    }
                }
            })

        if requests:
            result = service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={"requests": requests}
            ).execute()
            
            for reply in result.get("replies", []):
                if "addSheet" in reply:
                    props = reply["addSheet"]["properties"]
                    if props["title"] == "Analytics":
                        analytics_sheet_id = props["sheetId"]
                    elif props["title"] == "ChartData":
                        chart_data_sheet_id = props["sheetId"]

        existing_charts = service.spreadsheets().get(
            spreadsheetId=sheet_id
        ).execute()["sheets"]
        for sheet in existing_charts:
            if sheet["properties"]["title"] == "Analytics":
                if "charts" in sheet:
                    for chart in sheet["charts"]:
                        service.spreadsheets().batchUpdate(
                            spreadsheetId=sheet_id,
                            body={
                                "requests": [
                                    {"deleteEmbeddedObject": {"objectId": chart["chartId"]}}
                                ]
                            },
                        ).execute()

        sheet1_data = service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range="Sheet1!B2:D"
        ).execute().get("values", [])

        # pie chart data prep
        location_totals = defaultdict(int)
        for row in sheet1_data:
            if len(row) >= 3 and row[2].isdigit():
                location_totals[row[0]] += int(row[2])

        # stacked bar data prep
        store_names = sorted({row[0] for row in sheet1_data if len(row) >= 1})
        
        store_item_totals = defaultdict(lambda: defaultdict(int))
        for row in sheet1_data:
            if len(row) >= 3 and row[2].isdigit():
                store_item_totals[row[1]][row[0]] += int(row[2])

        item_totals = {item: sum(store_counts.values()) 
                      for item, store_counts in store_item_totals.items()
                      if item and item.strip()}
        
        top_items = sorted(item_totals.items(), key=lambda x: x[1], reverse=True)[:10]

        service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range="ChartData!A:Z"
        ).execute()

        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range="ChartData!A1",
            valueInputOption="RAW",
            body={
                "values": [
                    ["Store", "Total Waste"],
                    *[[store, location_totals[store]] for store in store_names]
                ]
            }
        ).execute()

        # top items data
        headers = ["Menu Item"] + store_names
        chart_data = [headers]
        for item, _ in top_items:
            row = [item]
            for store in store_names:
                row.append(store_item_totals[item].get(store, 0))
            chart_data.append(row)

        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range="ChartData!D1",
            valueInputOption="RAW",
            body={"values": chart_data}
        ).execute()

        # generate donut + stacked bar charts
        chart_requests = [
            {
                "addChart": {
                    "chart": {
                        "spec": {
                            "title": "Waste Count by Store",
                            "pieChart": {
                                "legendPosition": "RIGHT_LEGEND",
                                "pieHole": 0.4,
                                "domain": {
                                    "sourceRange": {
                                        "sources": [
                                            {
                                                "sheetId": chart_data_sheet_id,
                                                "startRowIndex": 1,  
                                                "endRowIndex": len(store_names) + 1,
                                                "startColumnIndex": 0,  
                                                "endColumnIndex": 1,
                                            }
                                        ]
                                    }
                                },
                                "series": {
                                    "sourceRange": {
                                        "sources": [
                                            {
                                                "sheetId": chart_data_sheet_id,
                                                "startRowIndex": 1,  
                                                "endRowIndex": len(store_names) + 1,
                                                "startColumnIndex": 1, 
                                                "endColumnIndex": 2,
                                            }
                                        ]
                                    }
                                },
                            },
                        },
                        "position": {
                            "overlayPosition": {
                                "anchorCell": {
                                    "sheetId": analytics_sheet_id,
                                    "rowIndex": 0,
                                    "columnIndex": 0
                                },
                                "offsetXPixels": 0,
                                "offsetYPixels": 0,
                                "widthPixels": 600,
                                "heightPixels": 400
                            }
                        }
                    }
                }
            },
            # stacked column
            {
                "addChart": {
                    "chart": {
                        "spec": {
                            "title": "Top 10 Most Wasted Items by Store",
                            "basicChart": {
                                "chartType": "COLUMN",
                                "legendPosition": "BOTTOM_LEGEND",
                                "stackedType": "STACKED",
                                "domains": [
                                    {
                                        "domain": {
                                            "sourceRange": {
                                                "sources": [
                                                    {
                                                        "sheetId": chart_data_sheet_id,
                                                        "startRowIndex": 1,  
                                                        "endRowIndex": len(top_items) + 1,
                                                        "startColumnIndex": 3, 
                                                        "endColumnIndex": 4,
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                ],
                                "series": [
                                    {
                                        "series": {
                                            "sourceRange": {
                                                "sources": [
                                                    {
                                                        "sheetId": chart_data_sheet_id,
                                                        "startRowIndex": 0,
                                                        "endRowIndex": len(top_items) + 1,
                                                        "startColumnIndex": i + 4,  # Start from column E
                                                        "endColumnIndex": i + 5,
                                                    }
                                                ]
                                            }
                                        },
                                        "targetAxis": "LEFT_AXIS"
                                    }
                                    for i in range(len(store_names))
                                ],
                                "headerCount": 1,
                                "axis": [
                                    {
                                        "position": "BOTTOM_AXIS",
                                        "title": "Menu Item"
                                    },
                                    {
                                        "position": "LEFT_AXIS",
                                        "title": "Waste Count"
                                    }
                                ],
                            },
                        },
                        "position": {
                        "overlayPosition": {
                            "anchorCell": {
                                "sheetId": analytics_sheet_id,
                                "rowIndex": 0,
                                "columnIndex": 8  
                            },
                            "offsetXPixels": 0,
                            "offsetYPixels": 0,
                            "widthPixels": 800,
                            "heightPixels": 400
                        }
                    }
                    }
                }
            }
        ]

        service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": chart_requests}
        ).execute()

        print("analytics sheet updated successfully!")

    except Exception as e:
        print(f"error updating analytics sheet: {e}")
        raise e


def get_sheet_id_by_name(service, spreadsheet_id, sheet_name):
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in spreadsheet['sheets']:
        if sheet['properties']['title'] == sheet_name:
            return sheet['properties']['sheetId']
    return None