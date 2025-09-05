#!/usr/bin/env python3
"""
Configuration file for the campaign tracking script.
Contains all necessary mappings and constants.
"""

# Google Sheet IDs
sheet_ids = {
    'whd-mar2025': '12IjTFI2Ebd1hyi-934093kaekjfkdljdf',
    'mc-jan2025': '1yqI-ajdflajdlfjakldfj',
    'yog-feb2025': 'ldkjflkjdflkjadlfj093i4',
    'tealbox-ombw-mar2025': 'ladjfloierjklvm,.cedf',
    'ombw-voucher': 'aldkfopieorpeqkdnf-akldjoier-J1u048Uat_7M',
    'anand-utsav-2025': 'lkdflkeoirjioerkejkfdjiofi'
    # Add more campaigns as needed
}

# Status types mapping
status_types = {
    'success': 5,
    'failed': 1
}

# Course types mapping
course_types = {
    'whd-mar2025': [684],
    'mc-jan2025': [684],
    'yog-feb2025': [875, 876, 877, 880],
    #'tealbox-ombw-mar2025': [684],
    # Add more campaigns as needed
}

# Course IDs mapping
course_ids = {
    'whd-mar2025': [834855, 834856, 834857, 834858, 834859, 834860, 681716],
    'mc-jan2025': [2390839,93894839,93940839048l9340983,939409384,0984348],
    'yog-feb2025': [813203, 813206, 813202, 848346, 848349, 848350],
    'tealbox-ombw-mar2025': [860540, 860541, 860542, 860543, 860544, 860545, 860546, 860547, 860548, 860549, 860550, 860551, 860552, 860553, 860555, 860556, 860557, 860558, 860559, 860560, 860561, 860562, 860563, 860564, 860565, 860566, 860567,882640,882641,882642,882643,882644,882645,882646,882647,882648,882649,882650,882651,882652,882653,882654],
    'ombw-voucher': [805191],
    'anand-utsav-2025':[882646,882647,882648]
    # Add more campaigns as needed
}

# Landing pages mapping
landing_pages = {
    'whd-mar2025': ["lp/happiness-program/whd", "lp/world-happiness-day", "lp/happiness/world"],
    'mc-jan2025': ["lp/meditation-course/jan2025", "lp/meditation-masterclass/jan2025"],
    'yog-feb2025': ["lp/yoga-program/feb2025", "lp/yoga-workshop/feb2025"],
    'tealbox-ombw-mar2025': ["online-meditation-breath-workshop/fb1", "online-meditation-breath-workshop/fb2", 
                        "online-meditation-breath-workshop/fb3", "online-meditation-breath-workshop/fb4","ombw-fb-1-v2",
                        "online-meditation-breath-workshop/fb5" ],
    'ombw-voucher': ["lp/ombw-redeem-voucher"],
    'anand-utsav-2025': ["lp/anand-utsav"],
    # Add more campaigns as needed
}

# Date filters mapping
date_filters = {
    'whd-mar2025': "cpd.`submitted_on` > '2025-03-03'",
    'mc-jan2025': "cpd.`submitted_on` > '2025-01-01'",
    'yog-feb2025': "cpd.`submitted_on` > '2025-02-22'",
    'tealbox-ombw-mar2025': "cpd.`submitted_on` > '2025-04-03'",
    'ombw-voucher': "cpd.`submitted_on` > '2025-03-23'",
    'anand-utsav-2025': "cpd.`submitted_on` > '2025-06-01'",
    # Add more campaigns as needed
}

# Exclude filters mapping - new configuration to exclude participants that show up in other tables
exclude_filters = {
    'tealbox-ombw-mar2025': "cpd.`id` not in (select lead_id from civicrm_voucher)",
    #'ombw-voucher': "NOT EXISTS (SELECT 1 FROM civicrm_course_participants_details cpd2 WHERE cpd2.participant_phone = cpd.participant_phone AND cpd2.entity_id IN (860540, 860541, 860542) AND cpd2.id != cpd.id)"
    # Add more campaign-specific exclusions as needed
}

# Database configuration
db_config = {
    'live': {
        'host': '65.0.22.33',
        'user': 'oeiroeikjdl',
        'password': 'lkasjflkjaslfj!',
        'database': 'klajdfkj'
    },
    'stage': {
        'host': '12.0.12.33',
        'user': 'dkfjjdflkj',
        'password': 'uaudiou@ejroejroi!',
        'database': 'djkfljlkdjfl'
    }
}

# Sheet names configuration by campaign
# Default sheet names (used if not specified in campaign_sheet_names)
default_sheet_names = {
    'course_success': 'Ctr Course Success',
    'course_failed': 'Ctr Course Failed',
    'ad_success': 'Ad LP Success',
    'ad_failed': 'Ad LP Failed'
}

# Campaign-specific sheet names (override defaults)
campaign_sheet_names = {
    'ombw-voucher': {
        'course_success': 'Voucher Success',
        'course_failed': 'Voucher Failed',
        'ad_success': 'Redeem LP Success',
        'ad_failed': 'Redeem LP Failed'
    },
    'anand-utsav-2025': {
        'course_success': 'AU Ctr Course Success',
        'course_failed': 'AU Ctr Course Failed',
        'ad_success': 'AU 2025 LP Success',
        'ad_failed': 'AU 2025 LP Failed'
    },
    
    # Add more campaign-specific sheet names as needed
}


# Merge sheets configuration
# If present, course_success and ad_success will be merged into merge_success sheet
# Similarly, course_failed and ad_failed will be merged into merge_failed sheet
# First column (Ref) will be used as key; in case of duplicates, ad/landing page data will be preferred
merge_sheet = {
    
    'tealbox-ombw-mar2025': {
        'merge_success': 'Funnel 1 All Success',
        'merge_failed': 'Funnel 1 All Failed'
    },
    'anand-utsav-2025': {
        'merge_success': 'AU 2025 All Success',
        'merge_failed': 'AU 2025 All Failed'
    }
    # Add more campaign-specific merge sheet names as needed
}
# Function to get sheet name based on campaign and sheet type
def get_sheet_name(campaign, sheet_type):
    """Get sheet name for a specific campaign and sheet type"""
    # Check if campaign has custom sheet names
    if campaign in campaign_sheet_names and sheet_type in campaign_sheet_names[campaign]:
        return campaign_sheet_names[campaign][sheet_type]
    # Otherwise use default sheet names
    return default_sheet_names[sheet_type]

# Column headers mapping
column_headers = {
    'common': ['Ref', 'Course ID', 'Course name', 'Name', 'Email', 'Phone', 'Submitted on', 'Status',
               'referal_site', 'reg_utm_url', 'first_page', 'last_page(greferrer)',
               'utm_campaign', 'utm_id', 'utm_source', 'utm_medium', 'utm_term',
               'utm_content', 'fbclid', 'reg_type']
}

# Juspay status codes mapping
juspay_status = {
    10: 'Newly created order. This is the status if transaction is not triggered for an order',
    20: 'Transaction is pending. Juspay system is not able to find a gateway to process a transaction',
    21: 'Successful transaction',
    22: 'User input is not accepted by the underlying PG',
    23: 'Authentication is in progress',
    26: 'User did not complete authentication',
    27: 'User completed authentication, but the bank refused the transaction.',
    28: 'Transaction status is pending from bank',
    29: 'COD Initiated Successfully',
    36: 'Transaction is automatically refunded'
}

# Google Sheets API configuration
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CREDENTIALS_FILE = './credentials.json'
TOKEN_FILE = './token.pickle'
prefill_key="sjkdfjlkjkldfjkldjfkleoripoerdmfk"
