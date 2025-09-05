#!/usr/bin/env python3
"""
Campaign tracking script to transfer participant data based on course registrations
and landing page visits to Google Sheets.

Usage:
    python campaign_tracker.py <campaign_name> [environment]
    
    Example:
    python campaign_tracker.py whd-mar2025
    python campaign_tracker.py mc-jan2025 stage
"""

import sys
import os
import pickle
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime
import urllib.parse
import json
import csv
import time
from typing import Dict, List, Set
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError

# Import configurations
from config_campaign import *

# Local backup and counter file paths
COURSE_SUCCESS_CSV_BACKUP = 'campaign_course_success_backup.csv'
COURSE_FAILED_CSV_BACKUP = 'campaign_course_failed_backup.csv'
AD_SUCCESS_CSV_BACKUP = 'campaign_ad_success_backup.csv'
AD_FAILED_CSV_BACKUP = 'campaign_ad_failed_backup.csv'
CAMPAIGN_COUNTER_FILE = 'campaign_counters.json'

# Set up logging
def setup_logging():
    """Configure logging for the script"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('campaign_tracker.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    # Set logging level to DEBUG to see all messages
    logging.getLogger().setLevel(logging.DEBUG)
    logging.info("Logging initialized with DEBUG level")

def load_local_counters() -> Dict[str, int]:
    """Load local counters from file."""
    try:
        if os.path.exists(CAMPAIGN_COUNTER_FILE):
            with open(CAMPAIGN_COUNTER_FILE, 'r') as f:
                counters = json.load(f)
                logging.debug(f"Loaded local counters: {counters}")
                return counters
        else:
            logging.info("No local counter file found - creating new counters")
            return {
                'course_success_count': 0, 'course_failed_count': 0,
                'ad_success_count': 0, 'ad_failed_count': 0,
                'total_processed': 0
            }
    except Exception as e:
        logging.error(f"Error loading local counters: {e}")
        return {
            'course_success_count': 0, 'course_failed_count': 0,
            'ad_success_count': 0, 'ad_failed_count': 0,
            'total_processed': 0
        }

def save_local_counters(counters: Dict[str, int]) -> None:
    """Save local counters to file."""
    try:
        with open(CAMPAIGN_COUNTER_FILE, 'w') as f:
            json.dump(counters, f, indent=2)
        logging.debug(f"Saved local counters: {counters}")
    except Exception as e:
        logging.error(f"Error saving local counters: {e}")

def verify_sheet_counts_against_local(existing_ids: Set[int], counters: Dict[str, int], sheet_type: str) -> bool:
    """
    Verify that Google Sheets counts match our local counters.
    
    Returns:
        Boolean indicating if counts match expectations
    """
    try:
        sheet_count = len(existing_ids)
        local_count = counters.get(f'{sheet_type}_count', 0)
        
        logging.info(f"Count verification for {sheet_type} - Sheets: {sheet_count}, Local: {local_count}")
        
        # Check for significant discrepancies
        diff = abs(sheet_count - local_count)
        
        if diff > 0:
            logging.error(f"*** ALERT: {sheet_type.upper()} COUNT MISMATCH *** Sheets: {sheet_count}, Local: {local_count}, Difference: {diff}")
            logging.error(f"*** CRITICAL ALERT: COUNT VERIFICATION FAILED FOR {sheet_type.upper()} ***")
            logging.error("*** THIS MAY INDICATE DATA LOSS OR DUPLICATION ***")
            logging.error("*** MANUAL REVIEW RECOMMENDED ***")
            return False
        else:
            logging.info(f"✓ Count verification PASSED for {sheet_type} - counts match")
            return True
            
    except Exception as e:
        logging.error(f"Error during count verification for {sheet_type}: {e}")
        return False

def update_counters_after_processing(counters: Dict[str, int], sheet_type: str, new_count: int, campaign: str, environment: str) -> Dict[str, int]:
    """Update local counters after successful processing."""
    updated_counters = counters.copy()
    updated_counters[f'{sheet_type}_count'] = counters.get(f'{sheet_type}_count', 0) + new_count
    updated_counters['total_processed'] = counters.get('total_processed', 0) + new_count
    updated_counters['last_updated'] = datetime.now().isoformat()
    updated_counters['campaign'] = campaign
    updated_counters['environment'] = environment
    
    logging.info(f"Updated counters: +{new_count} for {sheet_type}")
    return updated_counters

def save_to_csv(data: List[List], filename: str) -> None:
    """Save data to local CSV file."""
    try:
        if len(data) > 0:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(data)
            logging.info(f"Saved {len(data)-1 if len(data) > 1 else 0} records to {filename}")
    except Exception as e:
        logging.error(f"Error saving to CSV {filename}: {e}")
        raise

def verify_write_success(service, spreadsheet_id: str, sheet_name: str, written_data: List[List]) -> bool:
    """Verify that the records were actually written to the sheet."""
    try:
        if len(written_data) <= 0:
            logging.info("No records to verify")
            return True
            
        logging.info("Verifying write success by re-reading sheet...")
        
        # Wait a moment for API propagation
        time.sleep(3)
        
        # Re-read the sheet
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A:A"
        ).execute()
        
        values = result.get('values', [])
        
        current_ids = set()
        if len(values) > 1:
            for row in values[1:]:  # Skip header
                if row and row[0] and str(row[0]).replace('.', '').isdigit():
                    current_ids.add(int(float(row[0])))
        
        verification_failed = False
        
        # Check if all written IDs are now in the sheet
        for row in written_data:
            if row and len(row) > 0:
                try:
                    record_id = int(float(row[0]))
                    if record_id not in current_ids:
                        logging.error(f"VERIFICATION FAILED: ID {record_id} not found in sheet after write")
                        verification_failed = True
                except (ValueError, IndexError):
                    continue
        
        if not verification_failed:
            logging.info("Write verification PASSED - all records found in sheet")
            return True
        else:
            logging.error("Write verification FAILED - some records missing from sheet")
            return False
            
    except Exception as e:
        logging.error(f"Write verification error: {e}")
        return False

# Parse and validate command line arguments
def parse_arguments():
    """Parse command line arguments and validate them"""
    if len(sys.argv) < 2:
        logging.error("Required input is 'campaign_name'.")
        print("\nRequired input is 'campaign_name'.\n")
        sys.exit(1)
    
    campaign_name = sys.argv[1]
    environment = sys.argv[2] if len(sys.argv) > 2 else "live"
    
    # Validate campaign name
    if campaign_name not in sheet_ids:
        logging.error(f"Invalid campaign name '{campaign_name}'.")
        print(f"\nInvalid campaign name '{campaign_name}'.\n")
        sys.exit(1)
    
    # Validate environment
    if environment not in db_config:
        logging.error(f"Invalid environment '{environment}'.")
        print(f"\nInvalid environment '{environment}'.\n")
        sys.exit(1)
        
    logging.info(f"Starting script with campaign: {campaign_name}, environment: {environment}")
    return campaign_name, environment

# Database connection
def connect_to_database(environment):
    """Connect to the database based on environment setting"""
    try:
        config = db_config[environment]
        logging.info(f"Connecting to {environment} database at {config['host']}")
        
        connection = mysql.connector.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            database=config['database']
        )
        
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            db_info = connection.get_server_info()
            logging.info(f"Connected to MySQL server version {db_info}")
            return connection, cursor
        else:
            logging.error("Failed to connect to database")
            print("Failed to connect to database")
            sys.exit(1)
            
    except Error as e:
        logging.error(f"Error connecting to MySQL database: {e}")
        print(f"Error connecting to MySQL database: {e}")
        sys.exit(1)

# Google Sheets setup
def setup_google_sheets():
    """Set up Google Sheets API connection"""
    try:
        logging.info(f"Setting up Google Sheets API with token file: {TOKEN_FILE}")
        # Get or refresh credentials
        creds = None
        if os.path.exists(TOKEN_FILE):
            logging.info("Found existing token.pickle file")
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

# URL parameter extraction
def extract_utm_parameters(url, referal_site=""):
    """Extract UTM parameters and page info from URL"""
    params = {
        'first_page': '',
        'greferrer': '',
        'utm_campaign': '',
        'utm_id': '',
        'utm_source': '',
        'utm_medium': '',       # Added utm_medium
        'utm_term': '',         # Added utm_term
        'utm_content': '',      # Added utm_content
        'fbclid': ''            # Added fbclid
    }
    
    # Log input values for debugging
    logging.info(f"extract_utm_parameters called with url={url}, referal_site={referal_site}")
    
    # Try to extract from URL
    if url:
        try:
            parsed_url = urllib.parse.urlparse(url)
            query_params = urllib.parse.parse_qs(parsed_url.query)
            
            # Log parsed values
            logging.info(f"Parsed URL: {parsed_url}")
            logging.info(f"Query params: {query_params}")
            
            # Extract each parameter if it exists
            for key in params.keys():
                if key in query_params and query_params[key]:
                    params[key] = query_params[key][0]
            
            # Special handling for greferrer which is often in the URL params
            if 'greferrer' in query_params and query_params['greferrer']:
                params['greferrer'] = query_params['greferrer'][0]
                logging.info(f"Found greferrer in URL params: {params['greferrer']}")
                    
            # Extract first_page if not in query params but in URL path
            if not params['first_page'] and parsed_url.path:
                params['first_page'] = parsed_url.path.strip('/')
                
        except Exception as e:
            logging.warning(f"Error extracting parameters from URL: {e}")
    
    # Use referal_site as greferrer if not found in URL
    if not params['greferrer'] and referal_site:
        params['greferrer'] = referal_site
        logging.info(f"Used referal_site as greferrer: {params['greferrer']}")
    
    # Log final parameters
    logging.info(f"Extracted parameters: {params}")
        
    return params

# Registration type determination
def determine_reg_type(first_page, last_page, landing_pages_list):
    """Determine registration type based on first and last pages"""
    try:
        # Convert to strings if needed
        first_page_str = str(first_page) if first_page is not None else ""
        last_page_str = str(last_page) if last_page is not None else ""
        
        # Log inputs for debugging
        logging.info(f"determine_reg_type called with: first_page={first_page_str}, last_page={last_page_str}")
        logging.info(f"landing_pages_list={landing_pages_list}")
        
        # Initialize match flags
        first_match = False
        last_match = False
        
        # Check matches for first_page
        for lp in landing_pages_list:
            if lp in first_page_str:
                first_match = True
                logging.info(f"First page matched landing page: {lp}")
                break
                
        # Check matches for last_page
        for lp in landing_pages_list:
            if lp in last_page_str:
                last_match = True
                logging.info(f"Last page matched landing page: {lp}")
                break
        
        # Determine registration type
        if first_page_str == last_page_str and first_match:
            return "Direct"
        elif first_match and not last_match:
            return "Indirect"
        elif last_match:
            return "Direct"
        else:
            return "Other"
            
    except Exception as e:
        logging.error(f"Error in determine_reg_type: {e}")
        logging.error(f"Types - first_page: {type(first_page)}, last_page: {type(last_page)}")
        return "Error"

# Get existing IDs from sheet with robust error handling
def get_existing_sheet_ids(sheets_service, spreadsheet_id, sheet_name, retries: int = 3):
    """Get existing IDs from a sheet to avoid duplicates with retry logic"""
    existing_ids = set()
    
    for attempt in range(retries):
        try:
            logging.info(f"Attempt {attempt + 1}/{retries} to read existing IDs from {sheet_name}")
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, 
                range=f"{sheet_name}!A:A"
            ).execute()
            
            values = result.get('values', [])
            
            # Always assume the first row is a header and skip it
            if values and len(values) > 0:
                values.pop(0)
            
            # Extract and convert to numeric
            if values:
                for i, row in enumerate(values, start=2):  # Start from row 2 after header
                    if row and row[0]:  # Make sure there's a value
                        try:
                            if str(row[0]).strip().replace('.', '').isdigit():
                                existing_ids.add(int(float(row[0])))
                            else:
                                logging.warning(f"Non-numeric ID found in {sheet_name} row {i}: '{row[0]}'")
                        except (ValueError, TypeError, IndexError) as e:
                            logging.warning(f"Error processing {sheet_name} row {i}: {e}")
            
            logging.info(f"Successfully extracted {len(existing_ids)} IDs from {sheet_name}")
            return existing_ids
            
        except HttpError as e:
            logging.error(f"Google Sheets API error on attempt {attempt + 1} for {sheet_name}: {e}")
            if attempt < retries - 1:
                logging.info(f"Retrying in {2 ** attempt} seconds...")
                time.sleep(2 ** attempt)
            else:
                logging.error(f"All retry attempts failed for {sheet_name}")
                
        except Exception as e:
            logging.error(f"Unexpected error on attempt {attempt + 1} for {sheet_name}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    
    return existing_ids

# Build query for course-based data
def build_course_query(campaign, status_type, existing_ids):
    """Build SQL query for course-based data"""
    where_clauses = []
    
    # Add course types filter
    if campaign in course_types:
        course_type_str = ','.join(map(str, course_types[campaign]))
        where_clauses.append(f"ce.`event_type_id` IN ({course_type_str})")
        logging.info(f"Added course type filter with: {course_type_str}")
    
    # Add course ID filter
    if campaign in course_ids:
        course_id_str = ','.join(map(str, course_ids[campaign]))
        where_clauses.append(f"cpd.`entity_id` IN ({course_id_str})")
        logging.info(f"Added course ID filter with {len(course_ids[campaign])} IDs")
    
    # Add existing IDs filter to avoid duplicates
    if existing_ids:
        existing_ids_str = ','.join(map(str, existing_ids))
        where_clauses.append(f"(cpd.`id` NOT IN ({existing_ids_str}))")
        logging.info(f"Added existing IDs filter with {len(existing_ids)} IDs")
    
    # Add date filter if applicable
    if campaign in date_filters:
        where_clauses.append(date_filters[campaign])
        logging.info(f"Added date filter: {date_filters[campaign]}")
    
    # Add participant status filter - enhanced to handle both 5 and 19 for success
    if status_type == 'success':
        where_clauses.append("cpd.`participant_status` IN (5, 19)")
    else:
        status_code = status_types[status_type]
        where_clauses.append(f"cpd.`participant_status` = {status_code}")
    
    # Adjust for time difference between server and database
    where_clauses.append("cpd.`submitted_on` <= DATE_SUB(DATE_ADD(NOW(), INTERVAL 5.5 HOUR), INTERVAL 5 MINUTE)")
    
    # Add exclude filter if applicable (new feature)
    if campaign in exclude_filters and exclude_filters[campaign]:
        where_clauses.append(exclude_filters[campaign])
        logging.info(f"Added exclude filter: {exclude_filters[campaign]}")

    # Combine all WHERE clauses
    where_condition = ' AND '.join(where_clauses)
    
    # Full SQL query
    sql = f"""
    SELECT 
        cpd.`id`,
        cpd.`entity_id`,
        ce.`event_type_id`,
        ce.`id` as eid,
        ce.`title`,
        pei.`accounting_course_id_201`,
        CONCAT(cpd.`first_name`, ' ', cpd.`last_name`) AS `name`,
        cpd.`participant_email`,
        cpd.`participant_phone`,
        cpd.`pincode`,
        cpd.`submitted_on`,
        cpd.`referal_site`,
        cpd.`reg_utm_url`,
        pt.`pg_res_msg`,
        pt.`pg_res_code`,
        pt.`id` AS ptid
    FROM 
        `civicrm_course_participants_details` cpd
    INNER JOIN 
        `civicrm_event` ce ON ce.`id` = cpd.`entity_id`
    INNER JOIN 
        `civicrm_value_private_event_information_33` pei ON pei.`entity_id` = ce.`id`
    INNER JOIN 
        `payment_transactions` pt ON pt.`participant_id` = cpd.`id`
    WHERE {where_condition}
    GROUP BY cpd.`id`
    ORDER BY `submitted_on` ASC
    """
    
    logging.info("Course query built successfully")
    # Log the full SQL query
    logging.info(f"FULL SQL QUERY: {sql}")
    
    return sql

# Build query for landing page-based data
def build_landing_page_query(campaign, status_type, existing_ids):
    """Build SQL query for landing page-based data"""
    where_clauses = []
    
    # Add landing page filter - we need OR conditions for each landing page
    if campaign in landing_pages:
        landing_page_conditions = []
        for lp in landing_pages[campaign]:
            # Check multiple ways a landing page could appear in the URL
            landing_page_conditions.append(f"cpd.`reg_utm_url` LIKE '%{lp}%'")
            landing_page_conditions.append(f"cpd.`referal_site` LIKE '%{lp}%'")
            landing_page_conditions.append(f"cpd.`reg_utm_url` LIKE '%greferrer=%{lp}%'")
        
        landing_page_filter = f"({' OR '.join(landing_page_conditions)})"
        where_clauses.append(landing_page_filter)
        logging.info(f"Added landing page filter with {len(landing_pages[campaign])} pages and {len(landing_page_conditions)} conditions")
    
    # Add existing IDs filter to avoid duplicates
    if existing_ids:
        existing_ids_str = ','.join(map(str, existing_ids))
        where_clauses.append(f"(cpd.`id` NOT IN ({existing_ids_str}))")
        logging.info(f"Added existing IDs filter with {len(existing_ids)} IDs")
    
    # Add date filter if applicable
    if campaign in date_filters:
        where_clauses.append(date_filters[campaign])
        logging.info(f"Added date filter: {date_filters[campaign]}")
    
    # Add participant status filter - enhanced to handle both 5 and 19 for success
    if status_type == 'success':
        where_clauses.append("cpd.`participant_status` IN (5, 19)")
    else:
        status_code = status_types[status_type]
        where_clauses.append(f"cpd.`participant_status` = {status_code}")
    
    # Adjust for time difference between server and database
    where_clauses.append("cpd.`submitted_on` <= DATE_SUB(DATE_ADD(NOW(), INTERVAL 5.5 HOUR), INTERVAL 5 MINUTE)")
    
    # Combine all WHERE clauses
    where_condition = ' AND '.join(where_clauses)
    
    # Full SQL query
    sql = f"""
    SELECT 
        cpd.`id`,
        cpd.`entity_id`,
        ce.`event_type_id`,
        ce.`id` as eid,
        ce.`title`,
        pei.`accounting_course_id_201`,
        CONCAT(cpd.`first_name`, ' ', cpd.`last_name`) AS `name`,
        cpd.`participant_email`,
        cpd.`participant_phone`,
        cpd.`pincode`,
        cpd.`submitted_on`,
        cpd.`referal_site`,
        cpd.`reg_utm_url`,
        pt.`pg_res_msg`,
        pt.`pg_res_code`,
        pt.`id` AS ptid
    FROM 
        `civicrm_course_participants_details` cpd
    INNER JOIN 
        `civicrm_event` ce ON ce.`id` = cpd.`entity_id`
    INNER JOIN 
        `civicrm_value_private_event_information_33` pei ON pei.`entity_id` = ce.`id`
    INNER JOIN 
        `payment_transactions` pt ON pt.`participant_id` = cpd.`id`
    WHERE {where_condition}
    GROUP BY cpd.`id`
    ORDER BY `submitted_on` ASC
    """
    
    logging.info("Landing page query built successfully")
    # Log the full SQL query
    logging.info(f"FULL SQL QUERY: {sql}")
    
    return sql

# Process query results
def process_query_results(cursor, campaign, status_type, sheet_type, sql):
    """Process database query results and format for Google Sheets"""
    try:
        logging.info(f"Executing SQL query for {sheet_type} {status_type}")
        cursor.execute(sql)
        
        # For debugging, get the actual executed query
        last_executed_query = cursor._executed
        if isinstance(last_executed_query, bytes):
            last_executed_query = last_executed_query.decode('utf-8')
        logging.info(f"Actual executed query: {last_executed_query}")
        
        rows = cursor.fetchall()
        
        if not rows:
            logging.warning(f"No data found for {sheet_type} {status_type}.")
            return []
        
        logging.info(f"Query returned {len(rows)} rows")
        
        # Alert if processing unusually large batch
        if len(rows) > 100:  # Adjust threshold as needed
            logging.warning(f"ALERT: Processing unusually large batch of {len(rows)} records - this may indicate an issue")
        
        # Process results and prepare data for Google Sheets
        data = []
        
        # Get the landing pages for this campaign - ensure we're getting the right config
        campaign_landing_pages = landing_pages.get(campaign, [])
        logging.info(f"Campaign {campaign} landing pages: {campaign_landing_pages}")
        
        for row_index, row in enumerate(rows):
            logging.info(f"Processing row {row_index + 1} of {len(rows)}")
            
            # Ensure values are not None and convert phone to string immediately
            row['participant_email'] = row['participant_email'] or ""
            row['participant_phone'] = str(row['participant_phone']) if row['participant_phone'] else ""
            row['name'] = row['name'] or ""
            row['pincode'] = row['pincode'] or ""
            row['referal_site'] = row['referal_site'] or ""
            row['reg_utm_url'] = row['reg_utm_url'] or ""
            
            # Log row values for debugging
            logging.info(f"Row data - reg_utm_url: {row['reg_utm_url']}, referal_site: {row['referal_site']}")
            
            try:
                # Extract UTM parameters and first/last page
                utm_params = extract_utm_parameters(row['reg_utm_url'], row['referal_site'])
                
                # Get status text
                status_text = "Success" if status_type == "success" else "Failed"
                if status_type == "failed" and row['pg_res_msg']:
                    pg_status = juspay_status.get(row['pg_res_code'], '')
                    status_text = f"Failed: {row['pg_res_msg']} ({pg_status})"
                
                # Determine registration type
                reg_type = determine_reg_type(
                    utm_params['first_page'], 
                    utm_params['greferrer'], 
                    campaign_landing_pages  # Use the landing pages we verified
                )
                
                # Format data for Google Sheets
                formatted_row = [
                    row['id'],                      # Ref
                    row['accounting_course_id_201'],# Course ID
                    row['title'],                   # Course name
                    row['name'],                    # Name
                    row['participant_email'],       # Email
                    row['participant_phone'],       # Phone - now already a string
                    str(row['submitted_on']),       # Submitted on
                    status_text,                    # Status
                    row['referal_site'],            # referal_site
                    row['reg_utm_url'],             # reg_utm_url
                    utm_params['first_page'],       # first_page
                    utm_params['greferrer'],        # last_page(greferrer)
                    utm_params['utm_campaign'],     # utm_campaign
                    utm_params['utm_id'],           # utm_id
                    utm_params['utm_source'],       # utm_source
                    utm_params['utm_medium'],       # utm_medium
                    utm_params['utm_term'],         # utm_term
                    utm_params['utm_content'],      # utm_content
                    utm_params['fbclid'],           # fbclid
                    reg_type                        # reg_type
                ]
                
                data.append(formatted_row)
                logging.info(f"Successfully processed row {row_index + 1}")
                
            except Exception as e:
                logging.error(f"Error processing row {row_index + 1}: {e}")
                logging.error(f"Row data that caused error: {row}")
                # Use a fallback approach for this row
                formatted_row = [
                    row['id'],                      # Ref
                    row['accounting_course_id_201'],# Course ID
                    row['title'],                   # Course name
                    row['name'],                    # Name
                    row['participant_email'],       # Email
                    str(row['participant_phone']) if row['participant_phone'] else "",  # Phone - convert to string
                    str(row['submitted_on']),       # Submitted on
                    "Error",                        # Status
                    row['referal_site'],            # referal_site
                    row['reg_utm_url'],             # reg_utm_url
                    "",                             # first_page
                    row['referal_site'],            # last_page(greferrer)
                    "",                             # utm_campaign
                    "",                             # utm_id
                    "",                             # utm_source
                    "",                             # utm_medium
                    "",                             # utm_term
                    "",                             # utm_content
                    "",                             # fbclid
                    "Error"                         # reg_type
                ]
                data.append(formatted_row)
        
        return data
        
    except Error as e:
        logging.error(f"Error executing SQL query: {e}")
        print(f"Error executing SQL query: {e}")
        return []

def write_to_google_sheets_robust(service, spreadsheet_id: str, sheet_name: str, data: List[List], retries: int = 3) -> bool:
    """Write data to Google Sheets with enhanced error handling and verification."""
    if not data or len(data) == 0:
        logging.info(f"No data to write to {sheet_name}")
        return True
        
    for attempt in range(retries):
        try:
            logging.info(f"Attempt {attempt + 1}/{retries} to write {len(data)} rows to {sheet_name}")
            
            body = {'values': data}
            
            result = service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A2",  # Start from row 2 to preserve header
                valueInputOption='USER_ENTERED',  # Changed from 'RAW' to handle phone numbers better
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            
            updated_cells = result.get('updates', {}).get('updatedCells', 0)
            logging.info(f"Google Sheets API response for {sheet_name}: updatedCells={updated_cells}")
            
            # Verify the write was successful
            verification_passed = verify_write_success(service, spreadsheet_id, sheet_name, data)
            if not verification_passed:
                logging.error(f"CRITICAL: Write verification failed for {sheet_name} on attempt {attempt + 1}")
                if attempt < retries - 1:
                    logging.info("Retrying write operation...")
                    time.sleep(2 ** attempt)
                    continue
                else:
                    return False
            
            logging.info(f"Successfully wrote and verified data to {sheet_name}")
            return True
            
        except HttpError as e:
            logging.error(f"Google Sheets API error on attempt {attempt + 1} for {sheet_name}: {e}")
            if attempt < retries - 1:
                logging.info(f"Retrying in {2 ** attempt} seconds...")
                time.sleep(2 ** attempt)
            else:
                logging.error(f"All retry attempts failed for {sheet_name}")
                
        except Exception as e:
            logging.error(f"Unexpected error on attempt {attempt + 1} for {sheet_name}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            
    return False

# Append data to Google Sheets with robust error handling
def append_to_sheet(sheets_service, spreadsheet_id, sheet_name, data, sheet_type, campaign, environment):
    """Append data to Google Sheet with robust error handling and local backup"""
    if not data:
        logging.info(f"No data to append to sheet {sheet_name}")
        return 0
        
    try:
        # Determine CSV backup filename based on sheet type
        if 'course' in sheet_type and 'success' in sheet_type:
            csv_filename = COURSE_SUCCESS_CSV_BACKUP
        elif 'course' in sheet_type and 'failed' in sheet_type:
            csv_filename = COURSE_FAILED_CSV_BACKUP
        elif 'ad' in sheet_type and 'success' in sheet_type:
            csv_filename = AD_SUCCESS_CSV_BACKUP
        elif 'ad' in sheet_type and 'failed' in sheet_type:
            csv_filename = AD_FAILED_CSV_BACKUP
        else:
            csv_filename = f"campaign_{sheet_type}_backup.csv"
        
        # Save to local CSV backup
        save_to_csv(data, csv_filename)
        
        # Write to Google Sheets with robust error handling
        sheets_success = write_to_google_sheets_robust(sheets_service, spreadsheet_id, sheet_name, data)
        
        if sheets_success:
            updated_cells = len(data) * (len(data[0]) if data else 0)
            logging.info(f"Data added to sheet: {updated_cells} cells updated")
            print(f"Data added to {sheet_name}: {updated_cells} cells updated")
            
            # Update local counters after successful write and verification
            counters = load_local_counters()
            updated_counters = update_counters_after_processing(counters, sheet_type, len(data), campaign, environment)
            save_local_counters(updated_counters)
            
            logging.info(f"Successfully processed {len(data)} new {sheet_type} records")
            return updated_cells
            
        else:
            logging.error("CRITICAL: Failed to write to Google Sheets - data is saved to local CSV files")
            logging.error("Manual intervention may be required to prevent duplicate processing on next run")
            logging.error("*** LOCAL COUNTERS NOT UPDATED DUE TO WRITE FAILURE ***")
            print(f"CRITICAL ERROR: Failed to write to Google Sheets. Data saved to local CSV backup: {csv_filename}")
            return 0
        
    except Exception as e:
        logging.error(f"Error appending data to Google Sheets: {e}")
        print(f"Error appending data to Google Sheets: {e}")
        return 0

# Merge sheets function - improved to only write new rows
def merge_sheets(sheets_service, spreadsheet_id, campaign):
    """Merge course and landing page sheets if merge_sheet config is present"""
    # Check if merge_sheet config exists for this campaign
    if campaign not in merge_sheet:
        logging.info(f"No merge_sheet configuration found for campaign {campaign}, skipping merge")
        return 0
    
    merge_sheets_config = merge_sheet[campaign]
    logging.info(f"Found merge_sheet configuration for campaign {campaign}")
    
    total_new_merges = 0
    
    try:
        # Process each merge type (success and failed)
        for merge_type in ['success', 'failed']:
            # Get source sheet names
            course_sheet_name = get_sheet_name(campaign, f'course_{merge_type}')
            ad_sheet_name = get_sheet_name(campaign, f'ad_{merge_type}')
            
            # Get target merge sheet name
            merge_sheet_name = merge_sheets_config.get(f'merge_{merge_type}')
            if not merge_sheet_name:
                logging.warning(f"No merge_{merge_type} sheet name configured for campaign {campaign}")
                continue
                
            logging.info(f"Merging {course_sheet_name} and {ad_sheet_name} into {merge_sheet_name}")
            
            # Read data from course sheet with retry logic
            course_result = None
            for attempt in range(3):
                try:
                    course_result = sheets_service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id,
                        range=f"{course_sheet_name}!A:T"  # Include all columns (assuming 20 columns max)
                    ).execute()
                    break
                except HttpError as e:
                    logging.error(f"Error reading {course_sheet_name} on attempt {attempt + 1}: {e}")
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                    else:
                        logging.error(f"Failed to read {course_sheet_name} after all attempts")
            
            course_rows = course_result.get('values', []) if course_result else []
            if not course_rows:
                logging.warning(f"No data found in {course_sheet_name}")
                course_headers = []
                course_data = []
            else:
                # Separate headers and data
                course_headers = course_rows[0] if course_rows else []
                course_data = course_rows[1:] if len(course_rows) > 1 else []
                logging.info(f"Read {len(course_data)} rows from {course_sheet_name}")
            
            # Read data from ad/landing page sheet with retry logic
            ad_result = None
            for attempt in range(3):
                try:
                    ad_result = sheets_service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id,
                        range=f"{ad_sheet_name}!A:T"  # Include all columns (assuming 20 columns max)
                    ).execute()
                    break
                except HttpError as e:
                    logging.error(f"Error reading {ad_sheet_name} on attempt {attempt + 1}: {e}")
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                    else:
                        logging.error(f"Failed to read {ad_sheet_name} after all attempts")
            
            ad_rows = ad_result.get('values', []) if ad_result else []
            if not ad_rows:
                logging.warning(f"No data found in {ad_sheet_name}")
                ad_headers = []
                ad_data = []
            else:
                # Separate headers and data
                ad_headers = ad_rows[0] if ad_rows else []
                ad_data = ad_rows[1:] if len(ad_rows) > 1 else []
                logging.info(f"Read {len(ad_data)} rows from {ad_sheet_name}")
            
            # Use headers from either sheet (they should be the same)
            headers = course_headers if course_headers else ad_headers
            if not headers:
                logging.warning(f"No headers found in either sheet for {merge_type}, skipping merge")
                continue
            
            # Read existing merged data to avoid adding duplicates
            try:
                merge_result = sheets_service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=f"{merge_sheet_name}!A:T"  # Include all columns
                ).execute()
                
                merge_rows = merge_result.get('values', [])
                
                if not merge_rows:
                    logging.info(f"No existing data found in {merge_sheet_name}")
                    existing_headers = []
                    existing_data = []
                    existing_keys = set()
                else:
                    # Separate headers and data
                    existing_headers = merge_rows[0] if merge_rows else []
                    existing_data = merge_rows[1:] if len(merge_rows) > 1 else []
                    
                    # Get existing keys (from first column)
                    existing_keys = set()
                    for row in existing_data:
                        if row and len(row) > 0 and row[0]:
                            existing_keys.add(str(row[0]))
                    
                    logging.info(f"Found {len(existing_data)} existing rows in {merge_sheet_name} with {len(existing_keys)} unique keys")
                
            except Exception as e:
                # Sheet doesn't exist yet
                logging.info(f"Merge sheet {merge_sheet_name} does not exist yet: {str(e)}")
                existing_headers = []
                existing_data = []
                existing_keys = set()
                
                # Create the sheet
                sheet_body = {
                    'requests': [{
                        'addSheet': {
                            'properties': {
                                'title': merge_sheet_name
                            }
                        }
                    }]
                }
                
                try:
                    sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body=sheet_body
                    ).execute()
                    logging.info(f"Created new sheet {merge_sheet_name}")
                except Exception as create_error:
                    # If we fail here, it might be because the sheet exists with a different case
                    logging.warning(f"Error creating sheet (might already exist): {create_error}")
            
            # Create a dictionary to hold merged data, using first column (Ref) as key
            new_merged_data = {}
            
            # Add course data to merged_data if not already in existing_keys
            for row in course_data:
                if not row:  # Skip empty rows
                    continue
                    
                # Make sure row has a key (first column/Ref)
                if len(row) > 0 and row[0]:
                    key = str(row[0])  # Convert to string to ensure consistent key type
                    
                    # Only add if not already in existing data
                    if key not in existing_keys:
                        new_merged_data[key] = row
                        
                        # Pad row to match header length if needed
                        while len(new_merged_data[key]) < len(headers):
                            new_merged_data[key].append("")
            
            # Add ad/landing page data to merged_data if not already in existing_keys
            # (will overwrite course data in new_merged_data if same key)
            for row in ad_data:
                if not row:  # Skip empty rows
                    continue
                    
                # Make sure row has a key (first column/Ref)
                if len(row) > 0 and row[0]:
                    key = str(row[0])  # Convert to string to ensure consistent key type
                    
                    # Only add if not already in existing data
                    if key not in existing_keys:
                        new_merged_data[key] = row
                        
                        # Pad row to match header length if needed
                        while len(new_merged_data[key]) < len(headers):
                            new_merged_data[key].append("")
            
            # Convert new merged data back to list of rows
            new_merged_rows = list(new_merged_data.values())
            logging.info(f"Merged data has {len(new_merged_rows)} new rows to add")
            
            if not new_merged_rows:
                logging.info(f"No new rows to add to {merge_sheet_name}, skipping update")
                continue
            
            # If sheet is empty, write headers first
            if not existing_headers:
                # Write headers with retry logic
                for attempt in range(3):
                    try:
                        sheets_service.spreadsheets().values().update(
                            spreadsheetId=spreadsheet_id,
                            range=f"{merge_sheet_name}!A1",
                            valueInputOption='RAW',
                            body={'values': [headers]}
                        ).execute()
                        logging.info(f"Wrote headers to {merge_sheet_name}")
                        break
                    except HttpError as e:
                        logging.error(f"Error writing headers to {merge_sheet_name} on attempt {attempt + 1}: {e}")
                        if attempt < 2:
                            time.sleep(2 ** attempt)
                        else:
                            logging.error(f"Failed to write headers after all attempts")
            
            # Append new data to the merge sheet with robust error handling
            sheets_success = write_to_google_sheets_robust(sheets_service, spreadsheet_id, merge_sheet_name, new_merged_rows)
            
            if sheets_success:
                logging.info(f"Successfully merged data to {merge_sheet_name}: {len(new_merged_rows)} new rows")
                print(f"Merged data appended to {merge_sheet_name}: {len(new_merged_rows)} new rows")
                total_new_merges += len(new_merged_rows)
            else:
                logging.error(f"Failed to merge data to {merge_sheet_name}")
                print(f"Error: Failed to merge data to {merge_sheet_name}")
    
    except Exception as e:
        logging.error(f"Error merging sheets: {e}")
        print(f"Error merging sheets: {e}")
    
    return total_new_merges

# Main function
def main():
    """Main function to orchestrate the script execution"""
    setup_logging()
    campaign, environment = parse_arguments()
    
    connection = None
    cursor = None
    
    try:
        # Connect to database
        connection, cursor = connect_to_database(environment)
        
        # Set up Google Sheets
        sheets_service = setup_google_sheets()
        spreadsheet_id = sheet_ids[campaign]
        
        # Load local counters
        counters = load_local_counters()
        
        total_updates = 0
        
        # Process each sheet type
        for status in ['success', 'failed']:
            # Get the course sheet name for this campaign
            course_sheet_type = f'course_{status}'
            course_sheet_name = get_sheet_name(campaign, course_sheet_type)
            
            # Process course-based sheets
            existing_ids = get_existing_sheet_ids(sheets_service, spreadsheet_id, course_sheet_name)
            
            # Verify sheet counts against local counters
            count_verification_passed = verify_sheet_counts_against_local(existing_ids, counters, course_sheet_type)
            if not count_verification_passed:
                logging.warning(f"*** COUNT VERIFICATION FAILED FOR {course_sheet_type} - PROCEEDING WITH CAUTION ***")
            
            query = build_course_query(campaign, status, existing_ids)
            data = process_query_results(cursor, campaign, status, 'course', query)
            updated = append_to_sheet(sheets_service, spreadsheet_id, course_sheet_name, data, course_sheet_type, campaign, environment)
            total_updates += updated
            
            # Get the ad campaign sheet name for this campaign
            ad_sheet_type = f'ad_{status}'
            ad_sheet_name = get_sheet_name(campaign, ad_sheet_type)
            
            # Process ad campaign-based sheets
            existing_ids = get_existing_sheet_ids(sheets_service, spreadsheet_id, ad_sheet_name)
            
            # Verify sheet counts against local counters
            count_verification_passed = verify_sheet_counts_against_local(existing_ids, counters, ad_sheet_type)
            if not count_verification_passed:
                logging.warning(f"*** COUNT VERIFICATION FAILED FOR {ad_sheet_type} - PROCEEDING WITH CAUTION ***")
            
            query = build_landing_page_query(campaign, status, existing_ids)
            data = process_query_results(cursor, campaign, status, 'ad', query)
            updated = append_to_sheet(sheets_service, spreadsheet_id, ad_sheet_name, data, ad_sheet_type, campaign, environment)
            total_updates += updated
        
        # Merge sheets if merge_sheet config is present
        total_merges = 0
        if 'merge_sheet' in globals() and campaign in merge_sheet:
            logging.info(f"Starting merge process for campaign {campaign}")
            total_merges = merge_sheets(sheets_service, spreadsheet_id, campaign)
            logging.info(f"Completed merge process with {total_merges} total merged rows")
            print(f"Additionally, {total_merges} rows were merged into combined sheets")
        
        logging.info(f"Campaign tracking completed successfully with {total_updates} total updates")
        logging.info("*** PROCESSING COMPLETED SUCCESSFULLY ***")
        print(f"\nCampaign tracking completed successfully with {total_updates} total updates")
        
    except Exception as e:
        logging.error(f"CRITICAL ERROR in campaign tracking: {e}")
        logging.exception("Full traceback:")
        print(f"Critical error: {e}")
        return 1
        
    finally:
        # Always close database connections
        if cursor:
            cursor.close()
            logging.info("Database cursor closed")
        if connection and connection.is_connected():
            connection.close()
            logging.info("MySQL connection closed")
        
        logging.info("Script execution completed")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\nScript execution completed at {timestamp}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
