#!/usr/bin/env python3
"""
Facebook Conversion API fbclid Sender

This script reads Google Sheets containing lead or participant information with fbclid parameters,
sends the fbclid values to Facebook Conversion API for those that require processing,
and updates the Google Sheet with the results.

Usage:
    python fb_conversion_sender.py [configname]
    
    Example:
    python fb_conversion_sender.py default
    python fb_conversion_sender.py campaign_march
"""

import sys
import os
import pickle
import logging
import json
import time
from datetime import datetime
import requests
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Configuration variables
# Place your configuration details here
CONFIG = {
    'default': {
        'sheet_id': '',
        'sheets': [
            {
                'name': 'Ad LP Success', 
                'type': 'participants',      # Specifies this is a participants s sheet (process all rows)
                'check_converted': False
            },
            {
                'name': 'Masterclass',
                'type': 'leads',  # Specifies this is a leads sheet (process converted rows)
                'check_converted': True
            }
        ],
        'pixel_id': '',
        'access_token': '',
        'test_mode': False  # Set to False for production
    },
 'tealbox_mar_2025': {
        'sheet_id': 'alkdjfkjdflkjdkf',
        'sheets': [
            {
                'name': 'Ad LP Success', 
                'type': 'participants',      # Specifies this is a participants s sheet (process all rows)
                'check_converted': False
            },
            {
                'name': 'Masterclass',
                'type': 'leads',  # Specifies this is a leads sheet (process converted rows)
                'check_converted': True
            }
        ],
        'pixel_id': '8349384983948938',
        'access_token': 'ajdflkjadklfjkladjflkajdfkljdflkoeiroeropieopr',
        'test_mode': False  # Set to False for production
    },
}

# Constants
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
TOKEN_FILE = 'token.pickle'
CREDENTIALS_FILE = 'credentials.json'
FB_API_VERSION = 'v17.0'
MAX_ATTEMPTS = 5
BATCH_SIZE = 10  # Number of events to send in a batch
RATE_LIMIT_DELAY = 2  # Seconds to wait between API calls to avoid rate limiting

# Column mappings for different sheet types (0-indexed)
COLUMN_MAPS = {
    'leads': {
        'first_name': 23,
        'last_name': 22,
        'email': 2,
        'phone': 1,
        'fbclid': 10,
        'converted': 34,
        'fbclid_sent': 35,
        'attempts': 36,
        'utm_source': 4,
        'utm_medium': 9,
        'utm_campaign': 3,
        'utm_content': 7,
        'utm_term': 8,
        'url': 27,
        'ip_address': 15,
        'submitted_date': 29
    },
    'participants': {
        'name': 3,           # Full name in one column
        'email': 4,
        'phone': 5,
        'fbclid': 18,
        'fbclid_sent': 22,
        'attempts': 23,
        'utm_source': 14,
        'utm_medium': 15,
        'utm_campaign': 12,
        'utm_content': 17,
        'utm_term': 16,
        'url': 9,            # reg_utm_url
        'submitted_date': 6
    }
}

# Set up logging
def setup_logging():
    """Configure logging for the script"""
    logging.basicConfig(
        level=logging.DEBUG,  # Changed to DEBUG to capture more details
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('fb_conversion_sender.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.getLogger().setLevel(logging.DEBUG)  # Changed to DEBUG
    logging.info("Logging initialized with DEBUG level")

# Parse command line arguments
def parse_arguments():
    """Parse command line arguments"""
    config_name = 'default'
    if len(sys.argv) > 1:
        config_name = sys.argv[1]
    
    if config_name not in CONFIG:
        logging.error(f"Config name '{config_name}' not found in configuration.")
        print(f"\nConfig name '{config_name}' not found in configuration.\n")
        sys.exit(1)
    
    logging.info(f"Using configuration: {config_name}")
    return CONFIG[config_name]

# Get column mapping for a specific sheet type
def get_column_map(sheet_type):
    """Get the column mapping for the specified sheet type"""
    if sheet_type not in COLUMN_MAPS:
        logging.error(f"Unknown sheet type: {sheet_type}")
        print(f"Unknown sheet type: {sheet_type}")
        sys.exit(1)
    
    return COLUMN_MAPS[sheet_type]

# Google Sheets setup
def setup_google_sheets():
    """Set up Google Sheets API connection"""
    try:
        logging.info("Setting up Google Sheets API")
        
        # Get or refresh credentials
        creds = None
        if os.path.exists(TOKEN_FILE):
            logging.info("Found existing token file")
            with open(TOKEN_FILE, 'rb') as token:
                creds = pickle.load(token)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logging.info("Refreshing expired token")
                creds.refresh(Request())
            else:
                logging.info("Getting new token with OAuth flow")
                flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
            
            with open(TOKEN_FILE, 'wb') as token:
                pickle.dump(creds, token)
                logging.info("Token saved to pickle file")
        
        return build('sheets', 'v4', credentials=creds)
        
    except Exception as e:
        logging.error(f"Error setting up Google Sheets API: {e}")
        print(f"Error setting up Google Sheets API: {e}")
        sys.exit(1)

# Read data from Google Sheet
def read_sheet_data(service, config, sheet_name):
    """Read data from the Google Sheet"""
    try:
        logging.info(f"Reading data from sheet: {sheet_name}")
        
        result = service.spreadsheets().values().get(
            spreadsheetId=config['sheet_id'],
            range=f"{sheet_name}"
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            logging.warning(f"No data found in sheet {sheet_name}.")
            return [], []
        
        headers = values[0]
        data = values[1:] if len(values) > 1 else []
        
        logging.info(f"Found {len(data)} rows of data in {sheet_name} (excluding header)")
        return headers, data
        
    except Exception as e:
        logging.error(f"Error reading data from Google Sheet {sheet_name}: {e}")
        print(f"Error reading data from Google Sheet {sheet_name}: {e}")
        return [], []

# Filter rows that need fbclid to be sent
def filter_rows_to_process(data, column_map, check_converted=True):
    """Filter rows that need processing based on sheet type
    
    For leads sheet: Converted='Y', fbclid is not empty, fbclid_sent is not 'Y', attempts < MAX_ATTEMPTS
    For participants sheet: fbclid is not empty, fbclid_sent is not 'Y', attempts < MAX_ATTEMPTS
    """
    rows_to_process = []
    
    for index, row in enumerate(data):
        # Ensure the row has enough columns
        while len(row) <= max(column_map.values()):
            row.append("")
        
        # Get relevant values
        try:
            fbclid = row[column_map['fbclid']] if column_map['fbclid'] < len(row) else ""
            fbclid_sent = row[column_map['fbclid_sent']].upper() if column_map['fbclid_sent'] < len(row) and row[column_map['fbclid_sent']] else ""
            attempts_str = row[column_map['attempts']] if column_map['attempts'] < len(row) and row[column_map['attempts']] else "0"
            
            # Convert attempts to integer, default to 0 if not a valid number
            try:
                attempts = int(attempts_str)
            except ValueError:
                attempts = 0
            
            # Check converted status if required
            converted_check = True
            if check_converted:
                if 'converted' in column_map:
                    converted = row[column_map['converted']].upper() if column_map['converted'] < len(row) and row[column_map['converted']] else ""
                    converted_check = (converted == 'Y')
            
            # Check if row meets criteria for processing
            if (
                converted_check and 
                fbclid and 
                fbclid_sent != 'Y' and 
                attempts < MAX_ATTEMPTS
            ):
                rows_to_process.append((index + 2, row))  # +2 for 1-indexed row and header row
                logging.info(f"Row {index + 2} selected for processing - fbclid: {fbclid}")
            
        except Exception as e:
            logging.warning(f"Error processing row {index + 2}: {e}")
            continue
    
    logging.info(f"Found {len(rows_to_process)} rows to process")
    return rows_to_process

# Clean UTM parameters
def clean_utm_value(value):
    """Clean UTM parameter values by handling comma-separated and 'none' values"""
    if not value:
        return None
    
    # Take the first value if comma-separated
    cleaned_value = value.split(',')[0].strip()
    
    # Return None if the value is 'none'
    if cleaned_value.lower() == 'none':
        return None
    
    return cleaned_value

# Hash data for Facebook API (SHA256)
def hash_data(data):
    """Normalize and hash data for Facebook API"""
    if not data:
        return None
    
    import hashlib
    
    # Normalize: lowercase and remove whitespace
    normalized = data.lower().strip()
    
    # Hash using SHA256
    hashed = hashlib.sha256(normalized.encode('utf-8')).hexdigest()
    
    logging.debug(f"Hashed value: {data} -> {hashed}")
    return hashed

# Send fbclid to Facebook Conversion API
def send_to_facebook(row, row_index, column_map, config, sheet_type):
    """Send fbclid to Facebook Conversion API"""
    try:
        fbclid = row[column_map['fbclid']]
        logging.info(f"Sending fbclid {fbclid} to Facebook (row {row_index}) from {sheet_type} sheet")
        
        # Get submitted date/time for event_time
        event_time = int(time.time())  # Default to current time
        if column_map.get('submitted_date') is not None and column_map.get('submitted_date') < len(row) and row[column_map['submitted_date']]:
            try:
                # Try to parse the submitted date string to a timestamp
                submitted_date_str = row[column_map['submitted_date']]
                # Check common date formats - adjust as needed based on your date format
                for date_format in ('%Y-%m-%d %H:%M:%S', '%d-%m-%Y %H:%M:%S', '%m/%d/%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S'):
                    try:
                        dt_obj = datetime.strptime(submitted_date_str, date_format)
                        event_time = int(dt_obj.timestamp())
                        break
                    except ValueError:
                        continue
            except Exception as e:
                logging.warning(f"Could not parse submitted date, using current time: {e}")
        
        # Set event parameters - same for both sheet types
        event_name = 'Purchase V2'
        currency = 'INR'
        event_value = 1.0
        
        # Initialize user_data 
        user_data = {}
        
        # Add fbclid as fbc (do not hash per FB docs)
        if fbclid:
            user_data['fbc'] = fbclid
        
        # Add email if available and not empty
        if column_map.get('email') is not None and column_map.get('email') < len(row) and row[column_map['email']]:
            email_val = row[column_map['email']]
            hashed_email = hash_data(email_val)
            logging.info(f"Hashing email: {email_val} -> {hashed_email}")
            user_data['em'] = hashed_email
        
        # Add phone if available and not empty
        if column_map.get('phone') is not None and column_map.get('phone') < len(row) and row[column_map['phone']]:
            phone_val = row[column_map['phone']]
            hashed_phone = hash_data(phone_val)
            logging.info(f"Hashing phone: {phone_val} -> {hashed_phone}")
            user_data['ph'] = hashed_phone
        
        # Handle name fields differently based on sheet type
        if sheet_type == 'leads':
            # Leads sheet has separate first_name and last_name
            if column_map.get('first_name') is not None and column_map.get('first_name') < len(row) and row[column_map['first_name']]:
                fname_val = row[column_map['first_name']]
                hashed_fn = hash_data(fname_val)
                logging.info(f"Hashing first name: {fname_val} -> {hashed_fn}")
                user_data['fn'] = hashed_fn
            
            if column_map.get('last_name') is not None and column_map.get('last_name') < len(row) and row[column_map['last_name']]:
                lname_val = row[column_map['last_name']]
                hashed_ln = hash_data(lname_val)
                logging.info(f"Hashing last name: {lname_val} -> {hashed_ln}")
                user_data['ln'] = hashed_ln
                
        elif sheet_type == 'participants':
            # Participants sheet has a single name field
            if column_map.get('name') is not None and column_map.get('name') < len(row) and row[column_map['name']]:
                full_name = row[column_map['name']]
                # Try to split the name if possible
                name_parts = full_name.split(' ', 1)
                if len(name_parts) >= 2:
                    fname_val = name_parts[0]
                    lname_val = name_parts[1]
                    hashed_fn = hash_data(fname_val)
                    hashed_ln = hash_data(lname_val)
                    logging.info(f"Hashing first name from full name: {fname_val} -> {hashed_fn}")
                    logging.info(f"Hashing last name from full name: {lname_val} -> {hashed_ln}")
                    user_data['fn'] = hashed_fn
                    user_data['ln'] = hashed_ln
                else:
                    fname_val = full_name
                    hashed_fn = hash_data(fname_val)
                    logging.info(f"Hashing full name as first name: {fname_val} -> {hashed_fn}")
                    user_data['fn'] = hashed_fn
        
        # Add IP address if available (do not hash as per FB docs)
        if column_map.get('ip_address') is not None and column_map.get('ip_address') < len(row) and row[column_map['ip_address']]:
            user_data['client_ip_address'] = row[column_map['ip_address']]
        
        # Initialize custom_data with basic values
        custom_data = {
            'value': event_value,
            'currency': currency,
            'sheet_type': sheet_type  # Include sheet type as a custom parameter
        }
        
        # Add campaign parameters if available - with proper cleaning
        for utm_param in ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term']:
            if column_map.get(utm_param) is not None and column_map.get(utm_param) < len(row) and row[column_map[utm_param]]:
                cleaned_value = clean_utm_value(row[column_map[utm_param]])
                if cleaned_value:
                    custom_data[utm_param] = cleaned_value
        
        # Create the complete event data structure
        event_data = {
            'event_name': event_name,
            'event_time': event_time,
            'user_data': user_data,
            'custom_data': custom_data,
            'action_source': 'website'
        }
        
        # Add URL as event_source_url if available
        if column_map.get('url') is not None and column_map.get('url') < len(row) and row[column_map['url']]:
            event_data['event_source_url'] = row[column_map['url']]
        
        # Construct the API request
        url = f"https://graph.facebook.com/{FB_API_VERSION}/{config['pixel_id']}/events"
        
        payload = {
            'data': [event_data],
            'access_token': config['access_token']
        }
        
        # Add test_event_code if in test mode
        if config['test_mode'] and 'test_event_code' in config:
            payload['test_event_code'] = config['test_event_code']
        
        # Log the complete payload for debugging to verify hashing
        logging.info(f"Full Facebook API payload (unredacted): {json.dumps(payload)}")
        
        # Log a redacted version for general logging
        debug_payload = payload.copy()
        if 'data' in debug_payload and len(debug_payload['data']) > 0:
            if 'user_data' in debug_payload['data'][0]:
                debug_payload['data'][0]['user_data'] = {
                    k: ('***HASHED***' if k not in ['client_ip_address', 'client_user_agent', 'fbc', 'fbp'] else v)
                    for k, v in debug_payload['data'][0]['user_data'].items()
                }
        debug_payload['access_token'] = '***REDACTED***'
        logging.info(f"Redacted Facebook API payload: {json.dumps(debug_payload)}")
        
        # Make the API request
        response = requests.post(url, json=payload)
        result = {}
        
        try:
            if response.text:
                result = response.json()
            
            logging.info(f"Facebook API response status: {response.status_code}")
            logging.info(f"Facebook API response body: {response.text}")
            
            # Handle error responses
            if response.status_code != 200:
                error_message = f"API error {response.status_code}: {response.text}"
                logging.error(error_message)
                return False, error_message
            
            # Check if the event was successfully received
            if 'events_received' in result and result['events_received'] > 0:
                return True, "Success"
            else:
                error_msg = json.dumps(result.get('messages', ['Unknown error']))
                return False, f"API error: {error_msg}"
            
        except Exception as e:
            logging.error(f"Error parsing API response: {e}")
            return False, f"Error parsing response: {str(e)}"
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error sending to Facebook API: {e}")
        return False, f"Request error: {str(e)}"
        
    except Exception as e:
        logging.error(f"Error sending to Facebook API: {e}")
        return False, f"Error: {str(e)}"

# Update Google Sheet with results
def update_sheet(service, sheet_id, sheet_name, column_map, updates):
    """Update Google Sheet with results"""
    if not updates:
        logging.info(f"No updates to make to sheet {sheet_name}")
        return
    
    try:
        logging.info(f"Updating {len(updates)} rows in sheet {sheet_name}")
        
        batch_update_values_request_body = {
            'value_input_option': 'RAW',
            'data': []
        }
        
        for row_index, sent_status, attempts, error_message in updates:
            # Update fbclid_sent status
            fbclid_sent_col = chr(65 + column_map['fbclid_sent'])
            batch_update_values_request_body['data'].append({
                'range': f"{sheet_name}!{fbclid_sent_col}{row_index}",
                'values': [[sent_status]]
            })
            
            # Update attempts count
            attempts_col = chr(65 + column_map['attempts'])
            batch_update_values_request_body['data'].append({
                'range': f"{sheet_name}!{attempts_col}{row_index}",
                'values': [[attempts]]
            })
            
            # Add error message to next column (if needed)
            if error_message:
                next_col = chr(65 + column_map['attempts'] + 1)
                batch_update_values_request_body['data'].append({
                    'range': f"{sheet_name}!{next_col}{row_index}",
                    'values': [[error_message]]
                })
        
        # Execute the batch update
        result = service.spreadsheets().values().batchUpdate(
            spreadsheetId=sheet_id,
            body=batch_update_values_request_body
        ).execute()
        
        logging.info(f"Sheet {sheet_name} updated successfully: {result}")
        return len(updates)
        
    except Exception as e:
        logging.error(f"Error updating Google Sheet {sheet_name}: {e}")
        print(f"Error updating Google Sheet {sheet_name}: {e}")
        return 0

# Process a single sheet
def process_sheet(service, config, sheet_config):
    """Process a single sheet from the configuration"""
    sheet_name = sheet_config['name']
    sheet_type = sheet_config['type']
    check_converted = sheet_config.get('check_converted', True)
    
    logging.info(f"Processing sheet: {sheet_name} (type: {sheet_type})")
    
    # Get column mapping for this sheet type
    column_map = get_column_map(sheet_type)
    
    # Read data from sheet
    headers, data = read_sheet_data(service, config, sheet_name)
    
    if not data:
        logging.warning(f"No data found in sheet {sheet_name}")
        print(f"No data found in sheet {sheet_name}")
        return 0, 0, 0
    
    # Filter rows that need to be processed
    rows_to_process = filter_rows_to_process(data, column_map, check_converted)
    
    if not rows_to_process:
        logging.info(f"No rows need fbclid to be sent in sheet {sheet_name}")
        print(f"No rows need fbclid to be sent in sheet {sheet_name}")
        return 0, 0, 0
    
    # Process each row
    updates = []
    processed_count = 0
    success_count = 0
    error_count = 0
    
    for i, (row_index, row) in enumerate(rows_to_process):
        try:
            # Get current attempts value
            current_attempts = int(row[column_map['attempts']]) if row[column_map['attempts']] else 0
            new_attempts = current_attempts + 1
            
            # Send to Facebook
            success, message = send_to_facebook(row, row_index, column_map, config, sheet_type)
            
            # Prepare update data
            if success:
                updates.append((row_index, 'Y', new_attempts, ""))
                success_count += 1
                logging.info(f"Successfully sent fbclid for row {row_index}")
            else:
                updates.append((row_index, 'N', new_attempts, message))
                error_count += 1
                logging.warning(f"Failed to send fbclid for row {row_index}: {message}")
            
            processed_count += 1
            
            # Respect rate limits with a delay between batches
            if (i + 1) % BATCH_SIZE == 0:
                logging.info(f"Processed {i + 1} rows, waiting {RATE_LIMIT_DELAY} seconds...")
                time.sleep(RATE_LIMIT_DELAY)
            
        except Exception as e:
            logging.error(f"Error processing row {row_index}: {e}")
            # Still update the attempts count
            updates.append((row_index, 'N', new_attempts, f"Error: {str(e)}"))
            error_count += 1
    
    # Update the sheet with results
    update_sheet(service, config['sheet_id'], sheet_name, column_map, updates)
    
    # Return counts
    return processed_count, success_count, error_count

# Main function
def main():
    """Main function to orchestrate the script execution"""
    setup_logging()
    config = parse_arguments()
    
    try:
        # Set up Google Sheets API
        service = setup_google_sheets()
        
        # Track overall statistics
        total_processed = 0
        total_success = 0
        total_errors = 0
        
        # Process each sheet in the configuration
        for sheet_config in config['sheets']:
            sheet_name = sheet_config['name']
            sheet_type = sheet_config['type']
            
            logging.info(f"Starting to process sheet: {sheet_name} (type: {sheet_type})")
            
            # Process the sheet
            processed, success, errors = process_sheet(service, config, sheet_config)
            
            # Update totals
            total_processed += processed
            total_success += success
            total_errors += errors
            
            # Log sheet results
            logging.info(f"Sheet {sheet_name} processing complete: {processed} rows processed, {success} successful, {errors} errors")
            print(f"Sheet {sheet_name} processing complete:\n - {processed} rows processed\n - {success} successful\n - {errors} errors\n")
        
        # Print overall summary
        logging.info(f"All sheets processing complete: {total_processed} total rows processed, {total_success} total successful, {total_errors} total errors")
        print(f"\nAll sheets processing complete:\n - {total_processed} total rows processed\n - {total_success} total successful\n - {total_errors} total errors\n")
        
    except Exception as e:
        logging.error(f"Error in script execution: {e}")
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
