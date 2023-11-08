from flask import Flask, jsonify, render_template, request
import numpy as np
import json
from collections import namedtuple
import requests
import pandas as pd
from keal_estate import KealEstate
import logging


class ZipAPIFailed(Exception):
    """Raised when the Zipcode API fails."""
    pass

class GeocodeAPIFailed(Exception):
    """Raised when the Google Geocoding API fails."""
    pass

app = Flask(__name__)

with open('api_keys.json') as f:
    api_keys = json.load(f)

@app.route('/')
def home():
    return render_template('frontend.html', api_key=api_keys['GMAPS_KEY'])

GMAP_Format = namedtuple('GMAP_Format', ['address', 'geocode', 'rating', 'cashflow', 'listingURL'])


class GmapsInterlinker:
    """
    Interlinks requests from frontend and prettifies backend KealEstate information to be in a palatable format. 
    """
    def __init__(self):
        try:
            file_name = "near_zips.json"
            with open(file_name, 'r') as file:
                self.base_zip_to_near_zips = json.load(file)
        except FileNotFoundError:
            self.base_zip_to_near_zips = {}  # If the file does not exist, return an empty dictionary

    def _cashflow_to_rating(self, this_cashflow, cashflow_df):
        """
        Breaks down how "this_cashflow" number relates to the broader "cashflow_df" dataframe...
        A rating is given according to position in 
        20th percentile and less, 
        80th percentile and less, 
        100th percentile and less, 
        and positive cashflow
        """

        cashflow = cashflow_df['cashflow']
        np_cashflow = np.array(cashflow)

        # Calculate the percentiles
        top_20_percentile = np.percentile(np_cashflow, 80)
        bottom_20_percentile = np.percentile(np_cashflow, 20)
        
        #Correspond the position in broader dataframe with a number rating
        if(this_cashflow > 0): return 2
        if this_cashflow >= top_20_percentile: return 1
        elif this_cashflow > bottom_20_percentile: return 0
        else:
            return -1
    
    def _address_to_geocode(self, address):
        """
        Given and address "address", convert it to the gmaps geocded format to give to the front end javascript google maps api.
        """
        base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "address": address,
            "key": api_keys['GMAPS_KEY']
        }
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise GeocodeAPIFailed(f"Geocode API failed with error code {response.status_code} and message {response.json()}")

    def _reformat_for_frontend(self, cashflow_df):
        """
        Given the cashflow_df, converts each row into data palatable for the frontend 
        """
        listings = []
        for index, row in cashflow_df.iterrows():
            #Convert address to geocode.
            try:
                geocoded = self._address_to_geocode(row['formattedAddress'])
            except GeocodeAPIFailed: continue

            rating = self._cashflow_to_rating(row['cashflow'], cashflow_df)

            # Extract the specific latitude and longitude values
            lat = geocoded['results'][0]['geometry']['location']['lat']
            lng = geocoded['results'][0]['geometry']['location']['lng']

            # Create a namedtuple for each listing
            listing = GMAP_Format(
                address=row['formattedAddress'],
                geocode={'lat': lat, 'lng': lng}, 
                rating= rating,
                cashflow= row['cashflow'],
                listingURL=row['listingURL']
            )
            listings.append(listing._asdict())
        return listings

    def _get_near_zips(self, base_zip, distance_mi):
        """
        Given the base zipcode "base_zip" and the radiail distance "distance_mi" will return a list of neighboring zipcodes.
        """
        
        if(base_zip in self.base_zip_to_near_zips and len(self.base_zip_to_near_zips[base_zip]) > 0):
            return self.base_zip_to_near_zips[base_zip]
        
        api_key = api_keys["ZIPCODE_KEY"]
        format = 'json'
        zip_codes = str(base_zip)
        distance = distance_mi
        units = 'mile'

        #ind all US zip codes within a given radius of a zip code. Send a GET request to https://www.zipcodeapi.com/rest/<api_key>/radius.<format>/<zip_code>/<distance>/<units>.
        base_url = "https://www.zipcodeapi.com/rest/{}/radius.{}/{}/{}/{}".format(api_key, format, zip_codes, distance, units)

        response = requests.get(base_url)
        if response.status_code == 200:
            return self._process_zip_resp(response.json(), base_zip)
        else:
            raise ZipAPIFailed(f"Zip API Failed for zip {base_zip} with error code {response.status_code} and message {response.json()}")

    def _process_zip_resp(self, resp_json, base_zip):
        """
        Parses Zipcode API response and adds info into the "base_zip_to_near_zips" dictionary.
        """
        #convert and sort zipcodes to be by proximity
        df = pd.DataFrame(resp_json['zip_codes'])
        df_sorted = df.sort_values(by='distance')
        new_zipcodes = df_sorted['zip_code'].tolist()

        if base_zip in self.base_zip_to_near_zips:
            # Ensure new zip codes aren't already recorded
            for zip in new_zipcodes:
                if zip not in self.base_zip_to_near_zips[base_zip]:
                    self.base_zip_to_near_zips[base_zip].append(zip)
        else:
            # If base_zip not already in dictionary, just add new_zipcodes list
            self.base_zip_to_near_zips[base_zip] = new_zipcodes
        
        #Remove base zip from the list... if it's not in there, move on.
        try:
            self.base_zip_to_near_zips[base_zip].remove(base_zip)
        except ValueError:
            pass

        # Save back to the file
        file_name = "near_zips.json"
        with open(file_name, 'w') as file:
            json.dump(self.base_zip_to_near_zips, file)

        return self.base_zip_to_near_zips[base_zip]

    def _get_more_listings_in_nearby_zips(self, df, zip, amount_left, keal_estate):
        """
        Called when original zipcode didn't have enough listings. Calls neighboring zipcodes to fulfill the request.
        """
        gmaps_converter = GmapsInterlinker()

        #if we haven't gotten the amount we need, then find nearby zips and keep going.
        try:
            near_zips_list = gmaps_converter._get_near_zips(zip, 5)
        except ZipAPIFailed:
            pass
        else:
            #find neighboring zips to finish the job...
            if near_zips_list is not None and amount_left > 0:        
                keal_estate.listing_failure_count = 0 #reset failure_count since we are in a different zip now
                #go through each zipcode and look for more poperties
                for near_zip in near_zips_list:
                    new_df = keal_estate.get_cashflow_list(near_zip, amount_left)
                    new_df = new_df[~new_df['formattedAddress'].isin(df['formattedAddress'])].dropna()

                    #add newly found properties to the list and subtract from amount_left
                    if(len(new_df) > 0):
                        logging.debug(f"found something... len: {len(new_df)} zip {zip} it: {new_df}")
                        df = pd.concat([df, new_df], ignore_index=True)
        return df


@app.route('/request-markers/<zip>/<int:amount>', methods=['GET'])
def request_markers(zip, amount):
    """
    Returns formatted json string of all information required for frontend including:
    -geocded longitude/latitude
    -rating
    -address
    -cashflow
    -listing's URL
    """
    excluded_home_types = request.args.getlist('excludedHomeTypes', type=str)
    keal_estate = KealEstate(excluded_home_types) # Initialize your class
    gmaps_converter = GmapsInterlinker()

    # Get the base zips cashflow list...
    df = keal_estate.get_cashflow_list(zip, amount)
    
    #If we haven't gotten enough listings, get more in nearby zipcodes...
    amount_left = amount - len(df)
    logging.debug(f"amount left: {amount_left}")
    if amount_left > 0:
        df = gmaps_converter._get_more_listings_in_nearby_zips(df, zip, amount_left, keal_estate)
        
    # Transform your listings to the format your frontend needs
    transformed_listings = gmaps_converter._reformat_for_frontend(df) 

    # Return the data as JSON
    return jsonify(transformed_listings)

if __name__ == "__main__":
    app.run(debug=True) #or just app.run() if you don't want to use debug mode
