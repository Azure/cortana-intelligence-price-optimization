''' Add Web App site-packages to path so we can import them '''
import sys, os
sys.path.append('D:\\home\\site\\wwwroot\\env\\Lib\\site-packages')

import pandas as pd
from azure.storage.blob import BlockBlobService
from io import StringIO
import json
import numpy as np
from ast import literal_eval
from copy import deepcopy
from pyspark import SparkConf
from pyspark import SparkContext 
#from AttributeDescription import AttributeDescription
#from Store import Store
#from Inventory import Inventory
sc = SparkContext()






class AttributeDescription:
    def __init__(self):
        '''
        Load parameter file and create AttributeDescription
        @param string input_filename: local CSV file containing parameters, including blob storage account credentials
        @param bool load_from_blob: if true, loads an existing hierarchy on blob storage; otherwise generates a new one
        '''
           
        number_of_stores = 6
        number_of_brands = 3
        number_of_departments = 20
        number_of_weeks = 2
        storage_account_name = "<Storage-Account-Name>"
        storage_account_key = "<Storage-Account-Primary-Access-Key>"
        
        kwargs =  {"n_stores":number_of_stores, \
           "n_brands":number_of_brands, \
           "n_departments":number_of_departments, \
           "start_date":"2016-10-02 00:00:00", \
           "n_weeks":number_of_weeks, \
           "blob_account_name":storage_account_name, \
           "blob_account_key":storage_account_key, \
           "blob_raw_data_container":"rawdata", \
           "blob_public_parameters_container":"publicparameters", \
           "shipment_frequency":'1 days', \
           "shipment_size":300, \
           "shelf_life":'2 days', \
           "blob_private_parameters_container":"privateparameters"
           }
       
        self.hierarchy = {}
        self.hierarchy['InitialDate'] = kwargs['start_date']
        self.hierarchy['InitialWeeksToSimulate'] = kwargs['n_weeks']
        self.hierarchy['BlobAccountName'] = kwargs['blob_account_name']
        self.hierarchy['BlobAccountKey'] = kwargs['blob_account_key']
        self.hierarchy['BlobRawDataContainer'] = kwargs['blob_raw_data_container']
        self.hierarchy['BlobPublicParametersContainer'] = kwargs['blob_public_parameters_container']
        self.hierarchy['BlobPrivateParametersContainer'] = kwargs['blob_private_parameters_container']
        
        
        
        load_from_blob = self.connect_to_blob_storage()
        if load_from_blob:
            self.load_hierarchy()
            return
        
        '''
        If not loading from blob, need to generate new attributes for stores, products, etc.
        The assumptions used in generating these may change in the future.
        '''
        self.hierarchy['Stores'] = []
        price_elasticities = np.random.uniform(-1.5,-0.5, size=kwargs['n_departments']).tolist()
        desirabilities = np.random.uniform(0.7, 1.3, size=kwargs['n_brands']).tolist()
        msrps_and_costs = np.matrix(np.random.multivariate_normal(mean=[20, 10],
                                                                  cov=[[25, 6.25], [6.25,  6.25]],
                                                                  size=kwargs['n_brands']*kwargs['n_departments']).round(2))
        msrps_and_costs[:, 1] = np.apply_along_axis(max, 1, msrps_and_costs[:, 1], 1.)
        msrps_and_costs[:, 0] = np.maximum(msrps_and_costs[:, 1]*1.1, msrps_and_costs[:, 0])
        msrps_and_costs = msrps_and_costs.tolist()
        
        for StoreID in range(1, kwargs['n_stores']+1):
            AvgHouseholdIncome, AvgTraffic, LossRate = np.random.multivariate_normal(mean = [5E4, 100, 10],
                                                                                     cov = [[1E8, -1E4, 1E4],
                                                                                            [-1E4, 100, 10],
                                                                                            [1E4, 10, 4]]).tolist()
            store_dict = {}
            store_dict['StoreID'] = StoreID
            store_dict['AvgHouseholdIncome'] = AvgHouseholdIncome
            store_dict['AvgTraffic'] = AvgTraffic
            store_dict['Departments'] = []
            for DepartmentID in range(1, kwargs['n_departments']+1):
                department_dict = {}
                department_dict['DepartmentID'] = DepartmentID
                department_dict['PriceElasticity'] = price_elasticities[DepartmentID-1]
                department_dict['Brands'] = []
                for BrandID in range(1, kwargs['n_brands']+1):
                    brand_dict = {}
                    brand_dict['BrandID'] = BrandID
                    brand_dict['Desirability'] = desirabilities[BrandID-1]
                    brand_dict['Products'] = []
                    
                    ''' For the time being, only one product per brand-department combo '''
                    MSRP, Cost = msrps_and_costs[(DepartmentID - 1)*kwargs['n_brands'] + BrandID - 1]
                    product_dict = {}
                    product_dict['ProductID'] = '{}_{}'.format(DepartmentID, BrandID)
                    product_dict['Cost'] = round(Cost, 2)
                    product_dict['MSRP'] = round(MSRP, 2)
                    product_dict['LossRate'] = LossRate
                    product_dict['ShipmentFreq'] = kwargs['shipment_frequency']
                    product_dict['ShipmentSize'] = kwargs['shipment_size']
                    product_dict['ShelfLife'] = kwargs['shelf_life']
                    brand_dict['Products'].append(product_dict)
                    
                    department_dict['Brands'].append(brand_dict)
                store_dict['Departments'].append(department_dict)
            self.hierarchy['Stores'].append(store_dict)
        self.store_hierarchy()       # writes the JSON file to blob storage for later access
        self.write_csv_attributes()  # writes the same information in the original CSV form
        
        return


    def read_parameters_file(self, input_filename):
        ''' Read the CSV file listing parameters (created by hand or during CIQS) '''
        kwargs = {}
        try:
            input_file = open(input_filename, 'r')
        except IOError as e:
            raise Exception('Error opening parameter file {} for reading:\n{}'.format(input_filename, e))
        for line in input_file:
            try:
                parameter_name, parameter_value = line.strip().split(',')
                kwargs[parameter_name] = literal_eval(parameter_value)
            except Exception as e:
                raise Exception('Error parsing line of parameter file:\n{}\n{}'.format(line, e))
        return(kwargs)


    def connect_to_blob_storage(self):
        ''' Connects to blob storage and creates new containers (doesn't overwrite if present) '''
        self.block_blob_service = BlockBlobService(account_name=self.hierarchy['BlobAccountName'],
                                                   account_key=self.hierarchy['BlobAccountKey'])
        self.block_blob_service.create_container(self.hierarchy['BlobRawDataContainer'])
        self.block_blob_service.create_container(self.hierarchy['BlobPublicParametersContainer'])
        self.block_blob_service.create_container(self.hierarchy['BlobPrivateParametersContainer'])

        ''' Check if hierarchy.json file exists; if so, we can load from blob '''
        blob_list = [i.name for i in self.block_blob_service.list_blobs(self.hierarchy['BlobPrivateParametersContainer'])]
        if 'hierarchy.json' in blob_list:
            load_from_blob = True
        else:
            load_from_blob = False
        return load_from_blob

    
    def load_hierarchy(self):
        ''' Loads JSON-formatted hierarchy (from private container) '''
        hierarchy_string = self.block_blob_service.get_blob_to_text(self.hierarchy['BlobPrivateParametersContainer'],
                                                                    'hierarchy.json').content
        self.hierarchy = json.loads(hierarchy_string)
        return

    
    def store_hierarchy(self):
        ''' Write JSON-formatted hierarchy to both public and private containers '''
        hierarchy_string = json.dumps(self.hierarchy, sort_keys=True, indent=4, separators=(',', ': '))
        self.block_blob_service.create_blob_from_text(container_name = self.hierarchy['BlobPrivateParametersContainer'],
                                                        blob_name = 'hierarchy.json',
                                                        text = hierarchy_string)
        
        ''' Hide info that shouldn't be shared before writing to public container '''
        reduced_hierarchy = deepcopy(self.hierarchy)
        for i, store in enumerate(reduced_hierarchy['Stores']):
            for j, department in enumerate(store['Departments']):
                del department['PriceElasticity']
                for k, brand in enumerate(department['Brands']):
                    del brand['Desirability']
                    for ell, product in enumerate(brand['Products']):
                        del product['LossRate']
                        brand['Products'][ell] = product
                    department['Brands'][k] = brand
                store['Departments'][j] = department
            reduced_hierarchy['Stores'][i] = store
        hierarchy_string = json.dumps(reduced_hierarchy, sort_keys=True, indent=4, separators=(',', ': '))
        self.block_blob_service.create_blob_from_text(container_name = self.hierarchy['BlobPublicParametersContainer'],
                                                        blob_name = 'hierarchy.json',
                                                        text = hierarchy_string)
        return
    
    def write_csv_attributes(self):
        '''
        Under current assumptions about desirability, price elasticity, product offering, etc.
        constancy across stores and which features should remain hidden, writes:
        - stores.csv
        - brands.csv
        - departments.csv
        - products.csv
        This function will probably become obsolete if we decide to allow product offering,
        desirability, price elasticity, etc. to vary between stores.
        '''
        stores = []
        for store in self.hierarchy['Stores']:
            stores.append([store['StoreID'], store['AvgHouseholdIncome'], store['AvgTraffic']])
        store_df = pd.DataFrame(stores, columns=['StoreID', 'AvgHouseholdIncome', 'AvgTraffic'])
        self.block_blob_service.create_blob_from_text(container_name = self.hierarchy['BlobPublicParametersContainer'],
                                                      blob_name = 'stores.csv',
                                                      text = store_df.to_csv(index=False))
        
        ''' Currently, departments have the same properties across stores. We don't share price elasticity info. '''
        departments = []
        for department in self.hierarchy['Stores'][0]['Departments']:
            departments.append([department['DepartmentID']])
        department_df = pd.DataFrame(departments, columns=['DepartmentID'])
        self.block_blob_service.create_blob_from_text(container_name = self.hierarchy['BlobPublicParametersContainer'],
                                                      blob_name = 'departments.csv',
                                                      text = department_df.to_csv(index=False))
        
        ''' Currently, every brand offers a product in every department. We don't share desirability info. '''
        brands = []
        for brand in self.hierarchy['Stores'][0]['Departments'][0]['Brands']:
            brands.append([brand['BrandID']])
        brand_df = pd.DataFrame(brands, columns=['BrandID'])
        self.block_blob_service.create_blob_from_text(container_name = self.hierarchy['BlobPublicParametersContainer'],
                                                      blob_name = 'brands.csv',
                                                      text = brand_df.to_csv(index=False))
        
        products = []
        for department in self.hierarchy['Stores'][0]['Departments']:
            for brand in department['Brands']:
                product = brand['Products'][0]
                products.append([department['DepartmentID'], brand['BrandID'], product['ProductID'],
                                 product['MSRP'], product['Cost']])
        product_df = pd.DataFrame(products, columns=['DepartmentID', 'BrandID', 'ProductID', 'MSRP', 'Cost'])
        self.block_blob_service.create_blob_from_text(container_name = self.hierarchy['BlobPublicParametersContainer'],
                                                      blob_name = 'products.csv',
                                                      text = product_df.to_csv(index=False))
        return

    
    def get_product_features(self):
        ''' Loads product features in relational form '''
        features = []
        for store in self.hierarchy['Stores']:
            for department in store['Departments']:
                for brand in department['Brands']:
                    for product in brand['Products']:
                        features.append([store['StoreID'], store['AvgHouseholdIncome'], store['AvgTraffic'],
                                         department['DepartmentID'], department['PriceElasticity'],
                                         brand['BrandID'], brand['Desirability'],
                                         product['ProductID'], product['Cost'], product['MSRP'], product['LossRate'],
                                         product['ShelfLife'], product['ShipmentFreq'], product['ShipmentSize']])
        self.feature_df = pd.DataFrame(features, columns=['StoreID', 'AvgHouseholdIncome', 'AvgTraffic',
                                                          'DepartmentID', 'PriceElasticity', 'BrandID', 'Desirability',
                                                          'ProductID', 'Cost', 'MSRP', 'LossRate', 'ShelfLife',
                                                          'ShipmentFreq', 'ShipmentSize'])
        return
    
    
    def get_prices(self):
        ''' Load/generate prices for each product-store-date combination needed '''
        self.get_product_features()  # get the MSRP and Cost for each product-store combination, among other things
        price_change_df = None
        try:
            # Try loading the suggested price changes and merging with the product features.
            # This will fail during the initial round because there will be no suggested prices yet.
            price_change_string = self.block_blob_service.get_blob_to_text(self.hierarchy['BlobPublicParametersContainer'],
                                                                           'suggested_prices.csv')
            price_change_string = price_change_string.content
            price_change_df = pd.read_csv(StringIO(price_change_string), header=None,
                                          names=['ProductID', 'StoreID', 'DateTime', 'Price'])
            price_change_df['DateTime'] = pd.to_datetime(price_change_df['DateTime'])
            if price_change_df.DateTime.min() != price_change_df.DateTime.max():
                raise Exception('Could not load suggested_prices.csv because prices are suggested ' +
                                'for multiple dates (expected only one date).')
            price_change_df = self.feature_df.merge(price_change_df, on=['ProductID', 'StoreID'], how='left')
            price_change_df['DateTime'] = price_change_df['DateTime'].max()
        except Exception:
            print('Could not load suggested_prices.csv -- assuming this is the initial round')

        if price_change_df is None:
            # We need to do a full outer join between price change dates and product features.
            # Create a dummy column called "ones" for this purpose, and remove it afterward.
            dates = [pd.to_datetime(self.hierarchy['InitialDate']) + pd.to_timedelta('{} days'.format(7*i)) for i in range(self.hierarchy['InitialWeeksToSimulate'])]
            price_change_df = pd.DataFrame(dates, columns=['DateTime'])
            price_change_df['ones'] = 1
            feature_df = self.feature_df.copy(deep=True)
            feature_df['ones'] = 1
            price_change_df = feature_df.merge(price_change_df, on='ones', how='outer')
            price_change_df.drop('ones', axis=1, inplace=True)
            price_change_df['Price'] = np.NaN  # all prices must be randomly generated
        
        ''' Record which dates the U-SQL query will need to process '''
        #sales_start = price_change_df.DateTime.min() + pd.to_timedelta('1 days')  # first sales summary
        #sales_end = price_change_df.DateTime.max() + pd.to_timedelta('7 days')    #  last sales summary
        #output_str = 'SalesStart,SalesEnd\n{},{}\n'.format(sales_start.strftime('%Y-%m-%d %H:%M:%S'),
                                                           #sales_end.strftime('%Y-%m-%d %H:%M:%S'))
        #self.block_blob_service.create_blob_from_text(container_name = self.hierarchy['BlobPublicParametersContainer'],
                                                      #blob_name = 'unprocessed_dates.csv',
                                                      #text = output_str)

        ''' Randomly generate any unspecified prices '''
        price_change_df.Price = price_change_df.apply(self.choose_new_price, axis=1)
        
        ''' Store the prices and write to JSON files '''
        self.price_change_df = price_change_df[['ProductID', 'StoreID', 'DateTime', 'Price']]
        self.write_price_changes_to_blob()
        return


    def choose_new_price(self, row):
        ''' Pick a random price between a product's cost and MSRP '''
        if not np.isnan(row.Price):
            return(row.Price)
        mu = row.Cost + 0.8 * (row.MSRP - row.Cost)
        sd = (0.5 * (row.MSRP - row.Cost))**2
        result = np.random.normal(loc=mu, scale=sd)
        while (result > row.MSRP) or (result < row.Cost):
            result = np.random.normal(loc=mu, scale=sd)
        return(round(result, 2))
    

    def write_price_changes_to_blob(self):
        ''' Write one price change JSON file per store/date combination (after erasing the old ones) '''
        for blob in self.block_blob_service.list_blobs(self.hierarchy['BlobRawDataContainer']):
            if 'pc_' in blob.name:
                self.block_blob_service.delete_blob(self.hierarchy['BlobRawDataContainer'], blob.name)

        for record in self.price_change_df.groupby(['StoreID', 'DateTime']):
            blob_name = 'pc_store{}_{}.json'.format(record[0][0], record[0][1].strftime('%Y_%m_%d_%H_%M_%S'))

            ''' Create a dictionary of info to be encoded in JSON format '''
            price_change_dict = {}
            price_change_dict['StoreID'] = int(record[0][0])
            price_change_dict['PriceDate'] = str(record[0][1])
            entries = []
            for row in record[1].itertuples():
                entry_dict = {}
                entry_dict['ProductID'] = str(row.ProductID)
                entry_dict['Price'] = float(row.Price)
                entries.append(entry_dict)
            price_change_dict['PriceUpdates'] = entries

            output_str = json.dumps(price_change_dict, sort_keys=True, indent=4, separators=(',', ': '))
            self.block_blob_service.create_blob_from_text(container_name = self.hierarchy['BlobRawDataContainer'],
                                                    blob_name = blob_name,
                                                          text = output_str)
        return
    
 
    def get_demand(self):
        ''' Calculates demand values (currently based on a modified formula suggested by Yiyu) '''
        demand_df = self.price_change_df.merge(self.feature_df, on=['ProductID', 'StoreID'], how='left')
        demand_df['RelativePrice'] = demand_df['Price'] / demand_df.groupby('DepartmentID')['Price'].transform('mean')
        demand_df['FracDiscountOverMSRP'] = (demand_df.MSRP - demand_df.Price) / demand_df.MSRP
        demand_df['Demand'] = demand_df.AvgTraffic * demand_df.Desirability / (1 - demand_df.FracDiscountOverMSRP)
        demand_df.Demand += (demand_df.RelativePrice - 1) * demand_df.Price * demand_df.PriceElasticity
        demand_df.Demand /= demand_df.RelativePrice**2
        demand_df.Demand = demand_df.Demand.apply(lambda x: max(x, 5))
		#demand_df.Demand /= np.log(max(demand_df.Price, 2))**2
        #demand_df.Demand = 20 * demand_df.Demand.apply(lambda x: max(x, 0.01))
        demand_df = demand_df[['ProductID', 'StoreID', 'DateTime', 'Price', 'LossRate', 'ShelfLife', 'ShipmentFreq',
                               'ShipmentSize', 'Demand']]
        self.demand_df = demand_df
        return








class Inventory:
    ''' Helper class to report inventory/sales/losses '''
    def __init__(self, description, row):
        ''' Load last inventory record if possible; otherwise, create a new inventory '''
        self.block_blob_service = description.block_blob_service
        self.container = description.hierarchy['BlobRawDataContainer']
        self.store_id = row.StoreID
        self.product_id = row.ProductID
        self.price = row.Price
        self.shipment_size = row.ShipmentSize
        self.shipment_frequency = pd.to_timedelta(str(row.ShipmentFreq))
        self.shelf_life = pd.to_timedelta(str(row.ShelfLife))
        self.inventory = []
        self.arrivals = 0
        self.last_write_date = row.DateTime
        
        try:
            blob_name = 'inv_store{}_{}.json'.format(self.store_id,
                                                     row.DateTime.strftime('%Y_%m_%d_%H_%M_%S'))
            inv_string = self.block_blob_service.get_blob_to_text(self.container, blob_name).content
            last_inventory = json.loads(inv_string)
            for product in last_inventory['Products']:
                if product['ProductID'] == self.product_id:
                    break
            for batch in product['CurrentInventory']:
                self.inventory.append([pd.to_datetime(batch['ExpiryDateTime']), batch['Units']])
            self.start_date = self.inventory[0][0] - self.shelf_life  # maintain shipment phase
        except Exception as e:
            print('Could not load last inventory record {}; creating a brand new inventory'.format(blob_name))
            self.start_date = row.DateTime
            self.arrivals = self.shipment_size
            self.inventory = [[self.start_date + self.shelf_life, self.shipment_size]]
            
        self.sales = 0
        self.losses = 0
        self.spoilages = 0
        return
        
        
    def update_price(self, price):
        ''' Used to update the price included in sales records '''
        self.price = price
        return
    
    
    def remove_unit(self, time, is_sale = True):
        ''' Checks whether a new sale/loss event is possible, and if so, updates inventory and sale records '''
        time = time.round(freq='1s')
        if len(self.inventory) > 0:
            if is_sale:
                self.sales += 1
            else:
                self.losses += 1
            if self.inventory[0][1] == 1:
                self.inventory.pop(0) # last item from this expiry date; remove its entry
            else:
                self.inventory[0][1] -= 1
            event_dict = {}
            if is_sale:
                ''' Create a JSONable description of a sale (no need for loss events) '''
                event_dict['TransactionDateTime'] = str(time)
                event_dict['ProductID'] = self.product_id
                event_dict['Units'] = 1
                event_dict['Price'] = round(float(self.price), 2)
            return event_dict
        else:
            return None  # this return value indicates this item is sold out


    def end_of_day(self):
        ''' Receive shipments, remove expired products, write inventory record, and reset daily event tallies '''
        ''' Receive a shipment, if appropriate '''
        time_elapsed = pd.to_timedelta('1 days')
        current_write_date = self.last_write_date + time_elapsed
        if (current_write_date - self.start_date).days % self.shipment_frequency.days == 0:
            self.arrivals = self.shipment_size
            self.inventory.append([current_write_date + self.shelf_life, self.shipment_size])
        self.last_write_date = current_write_date
        
        ''' Remove any products now expired '''
        self.spoilages = sum([i[1] for i in self.inventory if i[0] <= self.last_write_date])
        self.inventory = [i for i in self.inventory if i[0] > self.last_write_date]
        
        ''' Write and reset sale/loss/arrival tallies '''
        inventory_summary = self.write_inventory_summary()
        self.arrivals = 0
        self.sales = 0
        self.losses = 0
        self.spoilages = 0
        return inventory_summary
        
    def write_inventory_summary(self):
        ''' Create a JSONable description of the inventory on this date '''
        inventory_dict = {}
        inventory_dict['ProductID'] = str(self.product_id)
        inventory_dict['Arrivals'] = int(self.arrivals)
        inventory_dict['Sales'] = int(self.sales)
        inventory_dict['Losses'] = int(self.losses)
        inventory_dict['Spoilages'] = int(self.spoilages)
        current_inventory = []
        for i in self.inventory:
            entry_dict = {}
            entry_dict['ExpiryDateTime'] = str(i[0])
            entry_dict['Units'] = int(i[1])
            current_inventory.append(entry_dict)
        inventory_dict['CurrentInventory'] = current_inventory
        return inventory_dict









class Store:
    def __init__(self, description, store_id):
        ''' Create a new store given the full simulation AttributeDescription and StoreID '''
        self.description = description
        self.container = description.hierarchy['BlobRawDataContainer']
        self.store_id = store_id
        
        '''
        Workday length and operating time is not currently a tunable parameter.
        Opening hours are hard-coded as 7 AM - 9 PM, seven days a week
        '''
        self.workday_length = 14./24
        self.opening_time = pd.to_timedelta('7 hours')
        self.closing_time = pd.to_timedelta('21 hours')
        self.todays_sales = []
        
        ''' Create an inventory for each product '''
        self.demand_df = description.demand_df.loc[description.demand_df.StoreID == store_id]
        self.product_ids = self.demand_df['ProductID'].unique()
        first_date = self.demand_df['DateTime'].min()
        self.inventories = {}
        for product_id in self.product_ids:
            row = self.demand_df.loc[(self.demand_df['ProductID'] == product_id) &
                                     (self.demand_df['DateTime'] == first_date)]
            self.inventories[product_id] = Inventory(description, row.iloc[0])
            
        ''' Now we can safely remove old sales/inv for this store from the rawdata container '''
        for blob in self.description.block_blob_service.list_blobs(self.container):
            if ('inv_store{}_'.format(store_id) in blob.name) or ('sales_store{}_'.format(store_id) in blob.name):
                self.description.block_blob_service.delete_blob(self.container, blob.name)  
        return


    def run(self):
        ''' Iterate through the dates, generating sales and loss events '''
        conversion_factor = 7 * self.workday_length
        for StartDate, date_df in self.demand_df.groupby('DateTime', sort=True):
            ''' Find the sales and loss rates for each product in that week '''
            rates = []
            events = []
            for row in date_df.itertuples():
                self.inventories[row.ProductID].update_price(row.Price)
                rates.extend([row.Demand, row.LossRate])
                events.extend([[row.ProductID, True], [row.ProductID, False]])
            ''' Beta is the expectation of inverse time until another event occurs '''
            beta = conversion_factor / sum(rates)
            rate_ids = list(range(len(rates)))
            probabilities = [i / sum(rates) for i in rates]
            
            for i in range(7):
                my_start_date = StartDate + i * pd.to_timedelta('1 days') + self.opening_time
                workday_elapsed = 0.
                
                while True:
                    ''' Choose a time elapsed until the next event '''
                    workday_elapsed += np.random.exponential(scale=beta)
                    if workday_elapsed > self.workday_length:
                        break
                        
                    ''' Choose which event occurred at that time and attempt it '''
                    event_id = np.random.choice(a=rate_ids, p=probabilities)
                    product_id, is_sale = events[event_id]
                    result = self.inventories[product_id].remove_unit(is_sale = is_sale,
                                                                      time = my_start_date + pd.to_timedelta(workday_elapsed, unit='d'))

                    ''' If a sale was successfully attempted (the item was in stock), record the sale '''
                    if (result is not None) and is_sale:
                        self.todays_sales.append(result)
                self.end_of_day()
        return


    def poor_mans_zero_truncated_poisson(self, k):
        ''' Draw x>=1 from a Poisson distribution (to determine # of items in a transaction) '''
        result = np.random.poisson(k)
        while (result == 0):
            result = np.random.poisson(k)
        return(result)


    def group_sales_into_transaction(self, sale_list):
        ''' Group individual item sales records into a single receipt '''
        products = []
        subtotal = 0.
        for sale in sale_list:
            entry_dict = {}
            entry_dict['ProductID'] = sale['ProductID']
            entry_dict['Price'] = sale['Price']
            products.append(entry_dict)
            subtotal += sale['Units'] * sale['Price']
        transaction = {}
        transaction['TransactionDateTime'] = sale_list[-1]['TransactionDateTime'] 
        transaction['Subtotal'] = round(subtotal, 2)
        transaction['Tax'] = round(subtotal * 0.07, 2)
        transaction['Total'] = round(subtotal + transaction['Tax'], 2)
        transaction['Products'] = products
        return(transaction)

    
    def end_of_day(self):
        ''' Write out sales transactions and inventory for the day '''
        ''' Begin by writing the inventory summary '''
        inventory_summaries = []
        for product_id in self.product_ids:
            inventory_summary = self.inventories[product_id].end_of_day()
            inventory_summaries.append(inventory_summary)
        write_date = self.inventories[self.product_ids[0]].last_write_date.strftime('%Y-%m-%d %H:%M:%S')
        write_date_file_format = self.inventories[self.product_ids[0]].last_write_date.strftime('%Y_%m_%d_%H_%M_%S')
        inventory_dict = {}
        inventory_dict['StoreID'] = int(self.store_id)
        inventory_dict['InventoryDateTime'] = write_date
        inventory_dict['Products'] = inventory_summaries
        inventory_blob_name = 'inv_store{}_{}.json'.format(self.store_id, write_date_file_format)
        inventory_blob_string = json.dumps(inventory_dict, sort_keys=True, indent=4, separators=(',', ': '))
        #self.description.block_blob_service.create_blob_from_text(self.container,
        #                                                          inventory_blob_name,
        #                                                          inventory_blob_string)

        ''' Now create the sales summary '''
        sales_dict = {}
        sales_dict['StoreID'] = int(self.store_id)
        sales_dict['SalesLogDateTime'] = write_date
        transactions = []
        idx = 0
        while(idx < len(self.todays_sales)):
            n = self.poor_mans_zero_truncated_poisson(2)  # max num of items in next transaction
            transactions.append(self.group_sales_into_transaction(self.todays_sales[idx:min(idx+n,
                                                                                    len(self.todays_sales))]))
            idx += n
        sales_dict['Transactions'] = transactions
        sales_blob_name = 'sales_store{}_{}.json'.format(self.store_id, write_date_file_format)
        sales_blob_string = json.dumps(sales_dict, sort_keys=True, indent=4, separators=(',', ': '))

        self.description.block_blob_service.create_blob_from_text(self.container,
                                                                  sales_blob_name,
                                                                  sales_blob_string)
        self.todays_sales = []
        return













    
    
    
    
    
    
    
    
''' Local classes for this simulation '''

if __name__ == '__main__':
    description = AttributeDescription()
    description.get_prices()
    description.get_demand()
    store_ids = description.demand_df['StoreID'].unique()
    ####################################output the processed_time_df.csv
    processed_time_df=pd.DataFrame([[description.demand_df.DateTime.min(),description.demand_df.DateTime.max()+pd.to_timedelta('7 days')]],columns=['startdate','enddate'])
    description.block_blob_service.create_blob_from_text(container_name = description.hierarchy['BlobPublicParametersContainer'],blob_name = 'processed_time_df.csv',text = processed_time_df.to_csv(index=False))
    ####################################output the processed_time_df.csv
    for store_id in store_ids:
        my_store = Store(description, store_id)
        my_store.run()

    

