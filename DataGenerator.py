import numpy as np
import pandas as pd
import sys

class DataGenerator:

    def __init__(self, sizeInBits):
        self.size = sizeInBits

    '''Genera n terminals'''
    def generate_terminal_profiles_table(self, n_terminals:int, random_state=0):
        
        np.random.seed(random_state)
            
        terminal_id_properties=[]
        
        # Generate terminal properties from random distributions 
        for terminal_id in range(n_terminals):
            
            x_terminal_id = np.random.uniform(0,100)
            y_terminal_id = np.random.uniform(0,100)
            
            terminal_id_properties.append([terminal_id,
                                        x_terminal_id, y_terminal_id])
                                        
        terminal_profiles_table = pd.DataFrame(terminal_id_properties, columns=['TERMINAL_ID',
                                                                        'x_terminal_id', 'y_terminal_id'])
        
        return terminal_profiles_table


    '''Genera n customers'''
    def generate_customer_profiles_table(self, n_customers:int, random_state=0):
        
        np.random.seed(random_state)
            
        customer_id_properties=[]
        
        # Generate customer properties from random distributions 
        for customer_id in range(n_customers):
            
            x_customer_id = np.random.uniform(0,100)
            y_customer_id = np.random.uniform(0,100)
            
            mean_amount = np.random.uniform(5,100) # Arbitrary (but sensible) value 
            std_amount = mean_amount/2 # Arbitrary (but sensible) value
            
            mean_nb_tx_per_day = np.random.uniform(0,4) # Arbitrary (but sensible) value 
            
            customer_id_properties.append([customer_id,
                                        x_customer_id, y_customer_id,
                                        mean_amount, std_amount,
                                        mean_nb_tx_per_day])
            
        customer_profiles_table = pd.DataFrame(customer_id_properties, columns=['CUSTOMER_ID',
                                                                        'x_customer_id', 'y_customer_id',
                                                                        'mean_amount', 'std_amount',
                                                                        'mean_nb_tx_per_day'])

        #print(sys.getsizeof(customer_profiles_table))

        return customer_profiles_table

    '''Restituisce i terminals usabili da un customer basando sulla posizione geografica tra customer e terminal nel raggio r'''
    def get_list_terminals_within_radius(self, customer_profile, x_y_terminals, r):
        
        # Use numpy arrays in the following to speed up computations
        
        # Location (x,y) of customer as numpy array
        x_y_customer = customer_profile[['x_customer_id','y_customer_id']].values.astype(float)
        #x = type(x_y_customer)

        # Squared difference in coordinates between customer and terminal locations
        squared_diff_x_y = np.square(x_y_customer - x_y_terminals)
        
        # Sum along rows and compute suared root to get distance
        dist_x_y = np.sqrt(np.sum(squared_diff_x_y, axis=1))
        
        # Get the indices of terminals which are at a distance less than r
        available_terminals = list(np.where(dist_x_y<r)[0])
        
        # Return the list of terminal IDs
        return available_terminals

    '''Genera le transactions di nb_days per customer'''
    def generate_transactions_table(self, customer_profile, start_date = "2018-04-01", nb_days = 10):
        
        customer_transactions = []
        
        np.random.seed(int(customer_profile.CUSTOMER_ID))
        np.random.seed(int(customer_profile.CUSTOMER_ID))
        
        # For all days
        for day in range(nb_days):
            
            # Random number of transactions for that day 
            nb_tx = np.random.poisson(customer_profile.mean_nb_tx_per_day)
            
            # If nb_tx positive, let us generate transactions
            if nb_tx>0:
                
                for tx in range(nb_tx):
                    
                    # Time of transaction: Around noon, std 20000 seconds. This choice aims at simulating the fact that 
                    # most transactions occur during the day.
                    time_tx = int(np.random.normal(86400/2, 20000))
                    
                    # If transaction time between 0 and 86400, let us keep it, otherwise, let us discard it
                    if (time_tx>0) and (time_tx<86400):
                        
                        # Amount is drawn from a normal distribution  
                        amount = np.random.normal(customer_profile.mean_amount, customer_profile.std_amount)
                        
                        # If amount negative, draw from a uniform distribution
                        if amount<0:
                            amount = np.random.uniform(0,customer_profile.mean_amount*2)
                        
                        amount=np.round(amount,decimals=2)
                        
                        if len(customer_profile.available_terminals)>0:
                            
                            terminal_id = np.random.choice(customer_profile.available_terminals)
                        
                            customer_transactions.append([time_tx+day*86400, day,
                                                        customer_profile.CUSTOMER_ID, 
                                                        terminal_id, amount])
                
        customer_transactions = pd.DataFrame(customer_transactions, columns=['TX_TIME_SECONDS', 'TX_TIME_DAYS', 'CUSTOMER_ID', 'TERMINAL_ID', 'TX_AMOUNT'])
        
        if len(customer_transactions)>0:
            customer_transactions['TX_DATETIME'] = pd.to_datetime(customer_transactions["TX_TIME_SECONDS"], unit='s', origin=start_date)
            customer_transactions=customer_transactions[['TX_DATETIME','CUSTOMER_ID', 'TERMINAL_ID', 'TX_AMOUNT','TX_TIME_SECONDS', 'TX_TIME_DAYS']]
        
        return customer_transactions  