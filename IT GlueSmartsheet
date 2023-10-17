import pandas as pd
import requests
import json
import time

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)

# Declare Global Variables

smartsheet_key = ""
itglue_key = ""
country_id = 2

smartsheet_url = ""
smartsheet_headers = {
    "Authorization": smartsheet_key,
    "Content-Type": "application/json",
}

itglue_url = "https://api.itglue.com/"
itglue_headers = {
    "x-api-key": itglue_key,
    "Content-Type": "application/vnd.api+json",
}

"""General get request form"""
def get_request(url, header):
    response = requests.get(url, headers=header)
    data = json.loads(response.text)
    return data

"""Remove duplicate parent offices from Smartsheet dataframe"""
def update_smartsheet_df(data, x):
    # Convert whole column to strings instead of integers/floats
    data["Location #"] = data["Location #"].map(str)
    
    # The value of X determines what changes will be made to the dataframe passed in
    # If 1 we are only looking at the first 3 of the site tag
    if x == 1:
        data["unique_id"] = data["Location #"].str[:4]
        df = data.groupby(["Parent Office", "unique_id"]).size().reset_index()
        return df
    
    # If 2 we are looking at the entire site tag
    elif x == 2:
        data["unique_id"] = data["Location #"].str[:6]
        return data
    
    # If no value or incorrect value for x, don't update the dataframe passed in
    else:
        return data

"""Create the Smartsheet dataframe"""
def smartsheet_df(data):
    columns = data["columns"]
    rows = data["rows"]
    
    # Extract column titles
    column_titles = ["row id"] + [col["title"] for col in columns]
    
    # Initialize an empty list to store row data
    data_rows = []
    
    for row in rows:
        values = [row["id"]]  # Initialize the row with the row id
        
        # Loop through cells to extract values
        for cell in row["cells"]:
            values.append(cell.get("value", None))  # Use .get() to handle missing values
            
        data_rows.append(values)
    
    # Create the dataframe
    df = pd.DataFrame(data_rows, columns=column_titles)
    
    return df


"""Create the IT Glue organization dataframe and add the unique id column"""
def itglue_org_df(data):
    df = pd.json_normalize(data["data"])
    df["unique_id"] = df["attributes.name"].str[:4]
    return df

"""Create the IT Glue location dataframe and add the unique id column"""
def itglue_loc_df(data):
    df = pd.json_normalize(data["data"])
    df["unique_id"] = df["attributes.name"].str[:6]
    return df

"""Merge the IT Glue dataframe with the Smartsheet dataframe (Smartsheet being source(df1))"""
def merge_dataframes(df1, df2, join, left_col, right_col):
    merged_df = pd.merge(df1, df2, how=join, left_on=left_col, right_on=right_col)
    return merged_df

"""Create an organization in IT Glue"""
def add_organization(office_name):
    # Create the data object to POST
    data = {
        'data': {
            'type': 'organizations',
            'attributes': {
                'name': office_name,
                'description': None,
                'organization-type-id': 165214,
                'organization-status-id': 47649,
                'quick-notes': None
            }
        }
    }
    
    # Set the URL for the API endpoint to add an organization
    url = f"{itglue_url}organizations/"
    
    # Send the POST request to add the organization
    response = requests.request("POST", url=url, headers=itglue_headers, json=data)
    
    # Check for success or failure
    print(response)

"""Add a location for a given organization"""
def add_location(name, address, address2, city, postal, region, phone_number, notes, org_id):
    # Create the data object to POST
    data = {
        "data": {
            "type": "locations",
            "attributes": {
                "organization_id": org_id,
                "name": name,
                "address_1": address,
                "address_2": address2,
                "city": city,
                "postal_code": postal,
                "region_id": region,
                "country_id": country_id,
                "phone": phone_number,
                "notes": notes
            },
            "relationships": {
                "organization": {
                    "data": {
                        "type": "organizations",
                        "id": org_id
                    }
                }
            }
        }
    }
    
    # Set the URL for the API endpoint to add a location
    url = f"{itglue_url}locations"
    
    # Send the POST request to add the location
    response = requests.request("POST", url=url, headers=itglue_headers, json=data)
    
    # Check for success or failure
    print(response)



def update_itglue_location_notes(smartsheet_df, loc_df):
    for index, row in smartsheet_df.iterrows():
        location_id = row["id"]  # Extract location ID
        organization_id = row["attributes.organization-id"]  # Extract organization ID
        
        # Find the corresponding location in IT Glue
        itglue_loc = loc_df[(loc_df["id"] == location_id) & (loc_df["attributes.organization-id"] == organization_id)]
        
        if not itglue_loc.empty:
            location_id = itglue_loc["id"].values[0]
        
        # Print debug information
        print(f"Processing location: {location_id}, organization: {organization_id}")
        
        # If the location is found, update its notes with Smartsheet data
        if location_id:
            # Construct the notes string
            notes = f"Office: {row['Office']}\nAbbreviation: {row['Abbreviation']}\nSpecialty: {row['Specialty']}\nOffice Manager: {row['Office Manager/ Primary Contact']}\nRegional Manager of Operations: {row['Regional Manager of Operations']}\nDirector of Operations: {row['Director of Operations']}\nVice President of Operations: {row['Vice President of Operations']}\nDirector of Marketing: {row['Director of Marketing']}\nPartner Doctors: {row['Partner Doctors']}\nAssociate Doctors: {row['Associate Doctors']}\nWebsite: {row['Website']}\nPractice Email: {row['Practice Email']}\nPMS Peds: {row['PMS Peds']}\nPMS Ortho: {row['PMS Ortho']}\nPMS OMS: {row['PMS OMS']}\nReal Estate: {row['Real Estate']}"
            
            # Prepare the data for updating
            data = {
                'data': {
                    'type': 'locations',
                    'id': location_id,
                    'attributes': {
                        'notes': notes
                    }
                }
            }
            
            # Set the URL for the API endpoint to update the location
            url = f"{itglue_url}locations/{location_id}"
            
            # Print debug information
            print(f"Updating location notes for location_id: {location_id}")
            
            # Send the PATCH request to update the location notes
            response = requests.request("PATCH", url=url, headers=itglue_headers, json=data)
            
            # Check for success or failure
            print(response)
        else:
            print(f"Location '{location_id}' not found in IT Glue.")


# Get all IT glue Organizations and turn into a dataframe
new_url = f"{itglue_url}organizations?page[size]=1000"
o = get_request(new_url, itglue_headers)
org_df = itglue_org_df(o)

# Get all IT glue Locations and turn into a dataframe
new_url = f"{itglue_url}locations?page[size]=1000"
l = get_request(new_url, itglue_headers)
loc_df = itglue_loc_df(l)

# Get all Smartsheet info and turn into a dataframe
s = get_request(smartsheet_url, smartsheet_headers)
smartsheet_temp = smartsheet_df(s)

# Create dataframe for organizations
org_smartsheet = update_smartsheet_df(smartsheet_temp, 1)

# Create dataframe for locations
loc_smartsheet = update_smartsheet_df(smartsheet_temp, 2)

# Merge the IT Glue and Smartsheet organization dataframes
merged_org_df = merge_dataframes(org_smartsheet, org_df, "left", ["unique_id"], ["unique_id"])
merged_loc_df = merge_dataframes(loc_smartsheet, loc_df, "left", ["unique_id"], ["unique_id"])

# Call the function to update IT Glue organizations' notes
update_itglue_location_notes(merged_loc_df, loc_df)
