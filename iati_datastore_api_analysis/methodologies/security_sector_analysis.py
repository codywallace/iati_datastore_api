from dotenv import load_dotenv
from pathlib import Path

# Standard python imports
import os
import urllib.parse
import json
import time
import re

# requests is a third party library you will have to install
import requests

# The codes we want to search for
dac_codes = [
    '15132', # Police
    '16063', # Narcotics Control
    '15261', # Child Soldiers (prevention and demobilisation)
    '15190', # Facilitation of Orderly, safe, regular and responsible migration and mobility
    '15230', # Participation in international peacekeeping operations
    '15136', # Immigration
    '15134', # Judicial Affairs
    '15130', # Legal and judicial development
    '15137', # Prisons
    '15210', # Security system management and reform
    '15131', # Justice, law and order policy, planning and administration
    '15111', # Public finance management (PFM)
    '15125', # Public Procurement
    '15120', # Public sector financial management
    '15113', # Anti-corruption organisations and institutions
    '15152', # Legislatures and political parties
    '15135', # Ombudsman
    '15163', # Media and free flow of information
    '15220', # Civilian peace-building, conflict prevention and resolution
    '15180',  # Ending Violence against Women and girls
    '15240',    # Reintegration and SALW control
    '15250',    # Removal of land mines and explosive remnants of war
    '16080',    # Social dialogue
    '15170',    # Women’s rights organisations and movements, and government institutions
    '15150',    # Democratic participation and civil society
    '15160',    # Human rights
    '15162',    # Human rights
    '15151',    # Elections
    '74010',    # Disaster risk prevention and preparedness
    '43060',    # Disaster Risk Reduction
    '15133',    # Fire and rescue services
]

# IATI API key - sign up at https://developer.iatistandard.org/
api_key = os.getenv("API_KEY")


# remember our removal policy https://iatistandard.org/en/data-removal/
# Any items that vanish from the data must also vanish from any copies you keep
# In this case this would involve cleaning out the "out" directory before each run

# Setup base_url
query = " OR ".join(["sector_code:"+dac_code for dac_code in dac_codes]) + " OR " + " OR ".join(["transaction_sector_code:"+dac_code for dac_code in dac_codes])
base_url = "https://api.iatistandard.org/datastore/activity/select?q="+ urllib.parse.quote_plus(query)+"&fl=iati_identifier,iati_json&rows=1000&sort=id ASC"
lookup_url = "https://codelists.codeforiati.org/api/json/en/ReportingOrganisation.json"

# Define a function to use to see if an activity should be included in the final results.
# This is needed because the search terms on the API don't allow us to express exactly what we want.
# So we have searched for slightly more than we want, and then we will double-check each item here
def in_activity_included(activity_json):
    # Get all the sector blocks we want to check
    items_to_check = activity_json.get("sector",[])
    for transaction in activity_json.get("transaction",[]):
        items_to_check.extend(transaction.get("sector",[]))
    # For each block, make sure the sector code and vocabulary matches
    # If it does, the activity should be included!
    for item_to_check in items_to_check:
        # Check the code is one we want, and the vocab is set to 1 (also these vocabs default to 1 if no value is set)
        if item_to_check.get("@code","").strip() in dac_codes and item_to_check.get("@vocabulary","1").strip() == '1':
            return True
    # If none match, this shouldn't be included
    return False

# Finally, loop to get the data and process it!
current_cursor = "NONE"
next_cursor = "*"
# We paginate till the current and next are the same - this indicates there are no more results
while next_cursor != current_cursor:
    print("Calling API ...")
    r = requests.get(base_url+"&cursorMark="+next_cursor, headers={"Ocp-Apim-Subscription-Key":api_key})
    if r.status_code != 200:
        print(r.status_code)
        raise Exception()
    print("Processing results ...")
    result_json = r.json()
    # process data
    for activity_solr_response in result_json.get("response",{}).get("docs",[]):
        activity_json = json.loads(activity_solr_response['iati_json']).get("iati-activity",[])[0]
        if in_activity_included(activity_json):
            # For now we just dump data to files
            # You'll want to change this to take the data into whatever system you are using
            #
            # Note this deals with cases where 2 bits of data returned from the API have the same iati_identifier by
            # just overwriting the earlier results and thus picking one at random
            #
            # Note also we have to change the iati_identifier a little bit to make the filename.
            # If processing from files, take the iati_identifier from the iati-identifier field in the JSON, NOT the filename.
            filename = re.sub(r'[<>:"/\\|?*]', '_', activity_solr_response['iati_identifier'].strip()) +".json"
            with open(os.path.join(RAW_DATA_DIR, filename), "w") as fp:
                try:
                    json.dump(activity_json, fp, indent=2)
                except Exception as e:
                    print(f"Error dumping JSON for {filename}: {e}")
    # Sort out details for next page
    current_cursor = next_cursor
    next_cursor = result_json.get("nextCursorMark")
    # Slow things down, or the API will block our requests
    time.sleep(1)


