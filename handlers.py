import logging
import glob
import requests
import time
from collections import namedtuple
import json
import pandas as pd
import datetime
import os
import time
from functools import partial

logging.basicConfig(level=logging.CRITICAL)

class NoMoreAgentsError(Exception):
    """Raised when there are no more agents to fetch."""
    pass

class ZillowAPIFailed(Exception):
    """Raised when the Zillow API call fails."""
    pass

ListingData = namedtuple('ListingData', ['formattedAddress', 'zip', 'beds', 'baths', 'price', 'zpid', 'homeType', 'listingURL'])

RentalData = namedtuple('RentalData', ['median',	'lowRent',	'highRent',
                                       		'percentile_25',	'percentile_75', 'comparableRents'])
PageData = namedtuple('PageData', ['current', 'max'])

with open('api_keys.json') as f:
    api_keys = json.load(f)

class ZillowAPIManager:
    MAX_RETRIES = 5
    RATE_LIMIT_PERIOD = 0.5  # 2 requests per second, so 0.5 seconds between requests

    @staticmethod
    def call_zillow_api(url_suffix, query_string, process_response):
        """
        Calls the Zillow API with the url_suffix appended to the end and the query_string passed through.
        """
        url = f"https://zillow-com1.p.rapidapi.com/{url_suffix}"

        headers = {
            "X-RapidAPI-Key": api_keys['ZILLOW_KEY'],
            "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
        }

        logging.info(f"Calling Zillow api for {url_suffix} with query {query_string}")

        last_call_time = 0

        for retry_count in range(ZillowAPIManager.MAX_RETRIES):
            # Wait if necessary to comply with rate limit
            time_since_last_call = time.time() - last_call_time
            if time_since_last_call < ZillowAPIManager.RATE_LIMIT_PERIOD:
                time.sleep(ZillowAPIManager.RATE_LIMIT_PERIOD - time_since_last_call)

            # Make the request
            response = requests.get(url, headers=headers, params=query_string)
            last_call_time = time.time()

            if response.status_code == 200:
                if url_suffix == "findAgent":
                    logging.critical(f"{url_suffix} response: {response.json()}\n\n")
                return process_response(response.json())
            else:
                logging.debug(f"API call to {url_suffix} failed with status code {response.status_code}, response {response} and query {query_string}. Retrying...")
                time.sleep(2 ** retry_count)  # Exponential backoff

        raise ZillowAPIFailed(f"API call to {url_suffix} failed with status code {response.status_code}, response {response} and query {query_string}")


class DataManager:
    @staticmethod
    def load_listing_data():
        listings = []
        zip_to_listings = {}
        #-) read listings csvs...
        # Read each CSV in the listing_data directory
        listings_directory = os.path.dirname(os.path.realpath(__file__)) + "/listing_data"
        for filename in os.listdir(listings_directory):
            logging.debug("Item in directory")
            if filename.endswith(".csv"):
                df = pd.read_csv(os.path.join(listings_directory, filename), dtype={'zip': str})
                for _, row in df.iterrows():
                    listing = ListingData(
                        formattedAddress=row['formattedAddress'],
                        zip=str(row['zip']),
                        beds=row['beds'],
                        baths=row['baths'],
                        price=row['price'],
                        zpid=row['zpid'],
                        homeType=row['homeType'],
                        listingURL=row['listingURL'],
                    )
                    logging.debug(row['zip'])
                    #If we alreayd have this listing, no need to add it again...
                    if listing not in listings:
                        listings.append(listing)

        #now take that big list of listings and parse it into the dictionary by zip code
        logging.debug("loading old listings")
        for listing in listings:
            if listing.zip not in zip_to_listings:
                zip_to_listings[listing.zip] = [listing]
            else:
                zip_to_listings[listing.zip].append(listing)
        return listings, zip_to_listings
    
    @staticmethod
    def load_agent_pages():
        #-) read zip_to_agentpages.json to find the page number of agents we are on!
        zip_to_agentpages = {}
        zippages_filename = f'zip_to_agentpages.json'
        if os.path.exists(zippages_filename):
            with open(zippages_filename, 'r') as f:
                data = json.load(f)

            # Convert each dictionary back into a PageData instance
            zip_to_agentpages = {str(k): PageData(**v) for k, v in data.items()}
        return zip_to_agentpages


class ListingsHandler:
    """
    All code relating to handling, fetching, maintaining Listing Data.
    """
    def __init__(self, excluded_hometypes):
        self.listing_failure_thresh = 3
        self.listing_failure_count = 0
        self.listings, self.zip_to_listings = DataManager.load_listing_data()
        self.excluded_hometypes = excluded_hometypes
    
    def get_listings(self, zip, amount):
        """
        returns a list with "amount" number of entries of listings in the requested "zip" zipcode.
        -Will return as many listings as possible for zip... will stop trying after failure_threshold is reached.

        """

        #-1) first check listings list to get as much from there as possible.
        if(zip in self.zip_to_listings and len(self._hometype_filtered_listings(self.zip_to_listings[zip])) >= amount):
            logging.debug("\nLISTINGS no need for api call")
            return self._hometype_filtered_listings(self.zip_to_listings[zip])
        
        #logging.debug(f"start count: {self.zip_to_listings[zip]}")
        return self._handle_listing_api(zip, amount)
    

    def _hometype_filtered_listings(self, listings):
        """
        This filters the given listings to remove the hometypes that were requested to be excluded.
        """
        desired =  [listing for listing in listings if listing.homeType.lower() not in self.excluded_hometypes]
        logging.debug(f"all {len(listings)}, desired {len(desired)}")
        return desired

    def _handle_listing_api(self, zip, amount):
        """
        Continues calling Zillow API for listings until the amount is reached, OR the failure threshold has been reached.
        """
        #Preparing for API Cal...

        #if zip not in zip to listings, init as empty list.
        if zip not in self.zip_to_listings:
            self.zip_to_listings[zip] = []

        #-2) call API with zuid save each listing as ListingData in listings list    
        #Keep looping to find more listings if:
        #1. We haven't found the amount we desire... still more to go
        #2. We haven't failed too many times yet
        while((len(self._hometype_filtered_listings(self.zip_to_listings[zip])) < amount) and self.listing_failure_count < self.listing_failure_thresh):
            logging.debug(f"\nNot enough listings, calling api again... count: {0 if zip not in self.zip_to_listings else len(self.zip_to_listings[zip])}" )
            
            #-3) call get_agent_zuid(zip) to get zuid
            try:
                agent_handler = AgentHandler()
                zuid = agent_handler.get_agent_zuid(zip)
                logging.debug(f"zuid: {zuid}")
            except (NoMoreAgentsError, ZillowAPIFailed) as e:
                #if there are no more agents, return what we have... nothing else we can do.
                return self._hometype_filtered_listings(self.zip_to_listings[zip])

            process_listing_api_partial = partial(self._process_listing_api_resp, zip = zip, zuid = zuid, agent_handler = agent_handler)
            #Call API
            try:
                ZillowAPIManager.call_zillow_api("agentActiveListings", {"zuid":f"{zuid}","page":"1"},process_listing_api_partial ) #can throw ZillowAPIFailed Error  
            except ZillowAPIFailed as e:
                continue
        

        return self._hometype_filtered_listings(self.zip_to_listings[zip])


    def _process_api_batch(self, api_listings):
        """
        Takes the received listings data from the API and converts it to the internal ListingData namedtuple type and returns the list.
        """
        batch = []
        for row in api_listings:
            # transform to namedtuple
            address = row['address']
            listing = ListingData(
                        formattedAddress=address['line1'] + " " + address['line2'],
                        zip=address['postalCode'],
                        beds=row['bedrooms'],
                        baths=row['bathrooms'],
                        price=row['price'],
                        zpid=row['zpid'],
                        homeType=row['home_type'],
                        listingURL=row['listing_url'],
                    )
            batch.append(listing)
        return batch

    def _process_listing_api_resp(self, resp_json, zip, zuid, agent_handler):
        """
        Parses the response from Zillow Listing API and saves the data, then appends it to the "zip_to_listings" dictionary
        """
        #Append new batch of listings to current set of listings.
        logging.debug(f"resp json {resp_json}")
        batch = self._process_api_batch(resp_json['listings'])
        self.listings = self.listings + batch

        failed = True
        for listing in batch:
            if listing.zip == zip: failed = False
            self.zip_to_listings[listing.zip] = self.zip_to_listings[listing.zip] + [listing] if listing.zip in self.zip_to_listings else [listing] 
        
        #"failed" refers to failing to find a listing of the requested zip... 
        if failed:
            self.listing_failure_count += 1
            logging.debug(f"failure count incr: {self.listing_failure_count}")

        self.save_batch_to_csv(batch, zip, zuid, agent_handler)

   
    def save_batch_to_csv(self, batch, zip, zuid, agent_handler):
        """
        If given batch isn't empty, save it to file.
        """
        if(len(batch) > 0):
            #-4) save this batch as a csv. (so that next time it gets added in step 1)
            batch_df = pd.DataFrame(batch, columns = ListingData._fields) #convert to dictionary then to DataFrame
            csv_filename = f"listing_data/listing_{zip}_{zuid}_page{agent_handler.zip_to_agentpages[zip].current}_{datetime.datetime.today().strftime('%Y%m%d')}.csv"
            # save contents
            batch_df.to_csv(csv_filename, index=False)

class AgentHandler:
    
    def __init__(self):
        self.zip_to_zuidlist = {}
        self.zip_to_agentpages = DataManager.load_agent_pages()

    def get_agent_zuid(self, zip):
        """
        Finds an agent associated with the given "zip" zipcode and returns the agent's ZUID
        """
        #-) check available_agents list
        #-) if  available...
        #---) take one and remove it from available_agents

        if zip not in self.zip_to_zuidlist: #if this zip isnt in zip_to_available agents...
            logging.debug("\tzip not in available agents")
            #initialize this zip as an empty list...
            self.zip_to_zuidlist[zip] = []

        if zip in self.zip_to_zuidlist and len(self.zip_to_zuidlist[zip]) > 0:
            # The pop(0) will remove the first agent in the list for the given zip_code.
            logging.debug("\tzip is in there and it exists")
            zuid = self.zip_to_zuidlist[zip].pop(0)
            return zuid
        
        if(not self.zip_to_agentpages or zip not in self.zip_to_agentpages): #if df is empty, or this item doesnt exist.
            logging.debug("\tdf empty, or item doesnt exist")
            #initialize this zip with a new default PageData...
            self.zip_to_agentpages[zip] = PageData(0,-1) #will be increased to zero to start.

        
        #otherwise if there is none saved... call api
        return self._handle_agent_api(zip)
    

    def _handle_agent_api(self,zip):
        """
        Calls Zillow API for Agents and outsources the parsing to method _process__agent_api_resp. Returns a zuid.
        """
        #Preparing for API call...

        #-Update page number in the dictionary to agent_pages(zip) += 1
        self.zip_to_agentpages[zip] = PageData(self.zip_to_agentpages[zip].current + 1, self.zip_to_agentpages[zip].max)
        #-Call API with zip updated page number for that zip.
        page = (self.zip_to_agentpages[zip].current)

        #If zip_to_agentpages exsits and the current page is past the max, throw NoMoreAgentsError. Can't go any further.
        z2ap_exists = zip in self.zip_to_agentpages and self.zip_to_agentpages[zip]
        if z2ap_exists and self.zip_to_agentpages[zip].current > self.zip_to_agentpages[zip].max and self.zip_to_agentpages[zip].max != -1:
            raise NoMoreAgentsError("No more agents to fetch for zip code... hit max pages {}".format(zip))


        process_agent_api_partial = partial(self._process_agent_api_resp, zip = zip)

        #Doing API call...
        return ZillowAPIManager.call_zillow_api("findAgent", {"locationText":zip, "page":page}, process_agent_api_partial)
        

    def _process_agent_api_resp(self, agent_resp_json, zip):
        """
        Adds all of the Agent API's response to "zip_to_agentpages" dictionary and then returns one zuid off the dictionary.
        """
        agents_data = agent_resp_json['agents']
        filter(None, agents_data)
        for agent in agents_data:
            zuid = agent['zuid']
            logging.debug(f"response zuid {zuid}")
            #add the zuid to the zip to zuid dictionary.
            self.zip_to_zuidlist[zip].append(zuid)


        page_info = agent_resp_json['pageInformation']
        last = page_info['lastPage']
        self.zip_to_agentpages[zip] = PageData(self.zip_to_agentpages[zip].current, last)

        # Convert each PageData to a dictionary
        converted = {str(k): v._asdict() for k, v in self.zip_to_agentpages.items()}

        # Save as a JSON file
        with open('zip_to_agentpages.json', 'w') as f:
            json.dump(converted, f)

        if len(self.zip_to_zuidlist[zip]) > 0:
            zuid = self.zip_to_zuidlist[zip].pop(0)
            return zuid
        else: 
            raise NoMoreAgentsError("The list zip_to_zuidlist[zip] is empty")  

    
class TaxHandler:

    @staticmethod
    def get_tax_data(listing_data):
        """
        Given a ListingData namedtuple, "listing_data", will return a TaxData namedtuple with the tax info on the given property.
        """
        #First check to see if file alrady exists, return that if true...
        found, tax_or_filename = TaxHandler._check_file_for_tax_data(listing_data)
        if found:
            return tax_or_filename
        #otherwise, call api and return THAT.
        try:
            return TaxHandler._handle_tax_api(listing_data, tax_or_filename)
        except ZillowAPIFailed:
            return 0

    @staticmethod
    def _check_file_for_tax_data(listing_data):
        """
        Returns success status in finding data on file and either the found tax number, 
        or the requested tax filename depending on the success status.
        """
        sanitized_address = listing_data.formattedAddress.replace('/', '_')
        tax_filename_pattern = f'tax_data/{sanitized_address}_tax_*.json'
        matching_files = glob.glob(tax_filename_pattern)

        #defaut tax_filename to what we WANT to name it if it doesn't exist...
        tax_filename = f'tax_data/{sanitized_address}_tax_{datetime.datetime.today().strftime("%Y%m%d")}.json'

        #if previous tax info on this property exists...
        if matching_files and os.path.exists(matching_files[0]):
            tax_filename = matching_files[0]
            with open(tax_filename, 'r') as f:
                tax_data = json.load(f)
                return True, tax_data['tax']
        
        return False, tax_filename


    @staticmethod
    def _handle_tax_api(listing_data, tax_filename):
        """
        Calls Zillow Tax API and outsources the processing of response to _process_tax_api_resp method. Returns tax estimate.
        """
        #-)if not... get zpid from address PropertyData and call API
        #--) save response as the most recent years tax payment and save to json

        process_tax_api_partial = partial(TaxHandler._process_tax_api_resp, tax_filename = tax_filename)
        return ZillowAPIManager.call_zillow_api("priceAndTaxHistory", {"zpid":listing_data.zpid}, process_tax_api_partial)
        
        
    @staticmethod
    def _process_tax_api_resp(resp_json, tax_filename):
        """
        Parses the Zillow Tax API response and returns either the found tax, or -1 if there were issues with calling the API or
        it's not on record.
        """
        tax_history = resp_json["taxHistory"]

        #Parse API response...
        for x in tax_history:
            if x["taxPaid"] is not None:
                tax = x["taxPaid"]
                with open(tax_filename, 'w') as f:
                    json.dump({'tax': tax}, f)  # Write the tax data to a JSON file
                return tax
            
        #save as -1 if not known.
        #For future support would like to have a "question mark" icon for partially missing information listings...
        with open(tax_filename, 'w') as f:
                    json.dump({'tax': -1}, f)  # Write the tax data to a JSON file
        return -1 


class RentalHandler:


    @staticmethod
    def get_rental_data(listing_data):
        """
        returns a RentalData namedtuple object given a ListingData namedtuple "listing_data"
        """

        found, rental_or_filename = RentalHandler._check_file_for_rental_data(listing_data)
        if found:
            return rental_or_filename 
        try:
            return RentalHandler._handle_rental_api(listing_data, rental_or_filename)
        except ZillowAPIFailed:
            return RentalData(0, 0, 0, 0, 0, 0)

    @staticmethod
    def _replace_none_with_zero(rental_data):
        # Use a dictionary comprehension to create a dictionary of replacements
        replacements = {field: 0 if value is None else value for field, value in rental_data._asdict().items()}
        # Use the _replace method with the ** operator to unpack the dictionary as keyword arguments
        return rental_data._replace(**replacements)

    @staticmethod
    def _check_file_for_rental_data(listing_data):
        """
        Returns success status in finding data on file and either the found rental number, 
        or the requested rental filename depending on the success status.
        """
        #-) Check if RentalData is already saved for that property in a csv.
        
        sanitized_address = listing_data.formattedAddress.replace('/', '_') #remove characters we can't save a file as.
        rental_filename_pattern = f'rental_data/{sanitized_address}_rental_*.json'
        matching_files = glob.glob(rental_filename_pattern)
        rental_filename = f'rental_data/{sanitized_address}_rental_{datetime.datetime.today().strftime("%Y%m%d")}.json'
    
        #-) if so... use read that from file and return it as a RentalData
        #if previous rental info exists of this address
        if matching_files and os.path.exists(matching_files[0]):
            rental_filename = matching_files[0]
            logging.debug(f"rental exists: {rental_filename}")
            # Load the dictionary from a JSON file
            with open(rental_filename, 'r') as f:
                rental_data_dict = json.load(f)
            # Convert the dictionary back to a namedtuple
            rental_data = RentalData(**rental_data_dict)
            rental_data = RentalHandler._replace_none_with_zero(rental_data)
            return True, rental_data

        return False, rental_filename

    @staticmethod
    def _format_prop_type(prop_type):
        """
        Takes the given "proprty type" from the ListingData and formats it for the Rental API call
        """
        prop_type = prop_type.lower()
        logging.debug(f"prop_type : {prop_type}")
        if prop_type == "townhome":
            prop_type = "Townhouse"
        if prop_type == "singlefamily":
            prop_type = "SingleFamily"
        if prop_type == "multifamily":
            prop_type = "MultiFamily"
        if prop_type == "condo":
            prop_type = "Condo"
        if prop_type == "townhouse":
            prop_type = "Townhouse"
        return prop_type

    @staticmethod
    def _handle_rental_api(listing_data, rental_filename):
        """
        Calls Zillow Rental API and returns a RentalData object. Outsources processing to _process_rental_api_resp
        """
        #Preparing for API call...

        #Converting the Property Type to the required format of the API query.
        prop_type = RentalHandler._format_prop_type(listing_data.homeType)
        full_address = listing_data.formattedAddress

        #Process API callback
        process_rental_api_partial = partial(RentalHandler._process_rental_api_resp, rental_filename = rental_filename)
        #API call...
        return ZillowAPIManager.call_zillow_api("rentEstimate", {"propertyType":prop_type,"address":full_address,"d":"0.5"}, process_rental_api_partial)
    
    
    @staticmethod
    def _process_rental_api_resp(response_json, rental_filename):
        """
        Given response from Rental API and returns a parsed RentalData namedtuple.
        """
        m = 0 if response_json["median"] == None else response_json["median"]
        rental_data = RentalData(median=m, lowRent=response_json['lowRent'], highRent=response_json['highRent'], percentile_25=response_json['percentile_25'], percentile_75=response_json['percentile_75'], comparableRents=response_json['comparableRentals'])

        #Initialize rental data to default numbers for sake of calculation.
        if rental_data is None or rental_data.median is None:
            rental_data = RentalData(0, 0, 0, 0, 0, 0)

        # Convert the namedtuple to a dictionary
        rental_data_dict = rental_data._asdict()

        # Save the dictionary as a JSON file
        with open(rental_filename, 'w') as f:
            json.dump(rental_data_dict, f)
        
        return rental_data