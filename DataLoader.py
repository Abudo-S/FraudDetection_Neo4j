from neo4j import GraphDatabase
import numpy as np
import pandas as pd

class DataLoader:
    #_driver:GraphDatabase.Driver

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        #print(self._driver.verify_connectivity())

    def run(self, query:str):
        self._driver.execute_query(query)
        self._driver.close()

    def importCustomers(self, customers:pd.DataFrame):
        query = "CREATE "
        for elementIdx in range(len(customers)):
            row = customers.iloc[elementIdx]
            query += f"(:Customer {{" \
                    f"CustomerID: {row['CUSTOMER_ID']}, " \
                    f"geo_location: [{row['x_customer_id']}, {row['y_customer_id']}], " \
                    f"spending_amount: {row['mean_amount']}, " \
                    f"spending_frequency: {row['mean_nb_tx_per_day']}" \
                    f"}})"
            
            if elementIdx != len(customers) -1:
                query += ', '
        
        self.run(query)

    def importTerminals(self, terminals):
        query = "CREATE "
        for elementIdx in range(len(terminals)):
            row = terminals.iloc[elementIdx]
            query += f"(:Terminal {{" \
                    f"TermID: {row['TERMINAL_ID']}, " \
                    f"geo_location: [{row['x_terminal_id']}, {row['y_terminal_id']}]" \
                    f"}})"
            
            if elementIdx != len(terminals) -1:
                query += ', '
        
        self.run(query)

    '''
    match (c:Customer{CustomerID: 75}) with c match (t:Terminal) where t.TermID in [15, 21, 60] create (c)-[:Use]->(t)
    '''
    def importCustomerTerminals(self, customer, terminalIds):
        query = "MATCH "

        if len(terminalIds) > 0:
            row = customer
            query += f"(c:Customer {{CustomerID: {row['CUSTOMER_ID']}}}) " \
                    f"WITH c  MATCH (t:Terminal) where t.TermID in [{', '.join([str(terminalId) for terminalId in terminalIds])}] " \
                    f"CREATE (c)-[:Use]->(t)" 
            
            self.run(query)
    
    ''' 
    Create (trans:Transaction{TransID: 50, datetime: '2024-01-01', amount: 50.2, isFraudulent: false});
    Match (c:Customer {CustomerID:72}), (trans:Transaction{TransID: 50}), (t:Terminal{TermID})
    Create (c)-[:Invoke]->(trans)-[:On]->(t)
    '''
    def importCustomerTransactions(self, transactions):
        #'TX_DATETIME','CUSTOMER_ID', 'TERMINAL_ID', 'TX_AMOUNT','TX_TIME_SECONDS', 'TX_TIME_DAYS'

        #query = ""
        for elementIdx in range(len(transactions)):
            row = transactions.iloc[elementIdx]
            trans_id = int(str(row['TERMINAL_ID']) + str(row['TX_TIME_SECONDS']))
            query = f"CREATE (:Transaction {{" \
                    f"TransID: {trans_id}, " \
                    f"datetime: '{row['TX_DATETIME']}', " \
                    f"amount: {row['TX_AMOUNT']}, " \
                    f"isFraudulent: false" \
                    f"}}); "
            
            self.run(query)

            query = f"MATCH (c:Customer {{CustomerID: {row['CUSTOMER_ID']}}})," \
                    f"(trans:Transaction {{TransID: {trans_id}}}),"\
                    f"(t:Terminal {{TermID: {row['TERMINAL_ID']}}})"\
                    f"CREATE (c)-[:Invoke]->(trans)-[:On]->(t)"
                    
            if elementIdx != len(transactions) -1:
                query += '; '
                
            self.run(query)
        

