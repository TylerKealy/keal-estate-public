import json
import pandas as pd
import datetime
import os
import logging
from handlers import ListingData, PageData, RentalData, ListingsHandler, RentalHandler, TaxHandler 

class PropertyUtility:
    @staticmethod
    def calculate_mortgage(principal, interest_rate, years):
        """
        Caclulates the mortgage of borrowing "principal" amount over "years" years at an interest rate of "interest_rate".
        """
        # Convert annual interest rate to monthly and a percentage
        monthly_rate = interest_rate / 12 / 100

        # Number of monthly payments
        n_payments = years * 12

        # Monthly mortgage payment
        mortgage_payment = principal * (monthly_rate * (1 + monthly_rate) ** n_payments) / ((1 + monthly_rate) ** n_payments - 1)

        return mortgage_payment


class KealEstate:

    def __init__(self, excluded_homeTypes = None):
        self.excluded_hometypes = [home_type.lower() for home_type in excluded_homeTypes] if excluded_homeTypes is not None else []
        
        self.listing_handler = ListingsHandler(excluded_homeTypes)

        #self.api_count = 0


    def get_cashflow_list(self, zip, property_count):
        """
        Returns a list of "property_count" size of properties within the given a zipcode "zip" with cashflow estimates calculated
        """
        listings = self.listing_handler.get_listings(zip, property_count)
        data = []
        #Adding extra columns to the ListingData namedtuple for our extra cashflow information.
        columns = list(ListingData._fields) + ['rent', 'expenses', 'tax', 'cashflow'] 
        for listing in listings:
            tax = TaxHandler.get_tax_data(listing)
            rental = RentalHandler.get_rental_data(listing)
            expenses = self.calculate_expenses(listing, tax, rental)

            #calculate cashflow as rental income - expenses...
            cashflow = rental.median - expenses

            #Add to cashflow list
            entry = listing._asdict()
            entry['rent'] = rental.median
            entry['expenses'] = expenses
            entry['tax'] = tax
            entry['cashflow'] = cashflow
            data.append(entry)
        
        #convert to dataframe and sort.
        df = pd.DataFrame(data, columns=columns)
        df_sorted = df.sort_values(by='cashflow', ascending=False)
        cashflowlist_name = f'cashflow_data/{zip}_count{property_count}_{datetime.datetime.today().strftime("%Y%m%d")}.csv'
        if not os.path.exists(cashflowlist_name):
            df_sorted.to_csv(cashflowlist_name, index=False)
        return df_sorted


        
    def calculate_expenses(self, listing_data: ListingData, tax, rental_data: RentalData):
        """
        Calculate the expenses of a listing "listing_data" given its tax and rental information, "tax" and "rental_data"
        """

        mortgage = PropertyUtility.calculate_mortgage(.8 * listing_data.price, 6.8 ,30)
        logging.debug(f"mortgage: {mortgage}")
        vacancy = .05 * rental_data.median
        logging.debug(f"vacancy: {vacancy}")
        repairs = .05 * rental_data.median
        logging.debug(f"repairs: {repairs}")
        tax /= 12
        logging.debug(f"tax: {tax}")
        capex = 183 #hardcoded
        logging.debug(f"capex: {capex}")
        mgmt = .11 * rental_data.median
        logging.debug(f"management: {mgmt}")
        total = mortgage + vacancy + repairs + tax + capex + mgmt
        logging.debug(f"total {total}")
        return total
