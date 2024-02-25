from neo4j import GraphDatabase
import datetime
from enum import Enum
import random

# class TransactionPeriod (Enum):
#     morning = 'morning', 
#     afternoon = 'afternoon', 
#     evening = 'afternoon', 
#     night = 'night'

# class TransactionProductType (Enum):
#     hightech = 'high-tech', 
#     food = 'food', 
#     clothing = 'clothing', 
#     consumable = 'consumable', 
#     other = 'other'

class DataElaborator():
    
    #_driver:GraphDatabase.Driver
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        #print(self._driver.verify_connectivity())

    def run(self, query:str):
        result = self._driver.session().run(query)
        #self._driver.close()
        data = []
        for record in result:
            data.append(dict(record))

        return data

    ''' pt.a
    match (c:Customer)-[:Invoke]-(t:Transaction) where t.datetime > '2024-01-25' 
    with c, collect(t) as transactions, sum(t.amount) as totAmounts 
    with c, size(transactions)/30 as spendingFrequency, totAmounts/30 as spendingAmount
    return c.CustomerID, spendingFrequency, spendingAmount, 
    CASE WHEN spendingFrequency < c.spending_frequency  and spendingAmount < c.spending_amount THEN true ELSE false END AS underTheLimit
    '''
    def getCustomerLastMonthLimit(self):
        query = "MATCH "
        query += f"(c:Customer)-[:Invoke]-(t:Transaction) where t.datetime > '{str(datetime.datetime.now().date() - datetime.timedelta(days=31))}' "\
                f"with c, collect(t) as transactions, sum(t.amount) as totAmounts "\
                f"with c, size(transactions)/30 as spendingFrequency, totAmounts/30 as spendingAmount "\
                f"return c.CustomerID, spendingFrequency, spendingAmount, "\
                f"CASE WHEN spendingFrequency < c.spending_frequency  and spendingAmount < c.spending_amount THEN true ELSE false END AS underTheLimit"
        
        return self.run(query)
    
    '''pt.b
    match (t:Terminal) -[:On]- (tx:Transaction) where tx.datetime > '2024-01-26'
    with t, 0.2 * max(tx.amount) as threshold, collect(tx.TransID) as transactionIds
    match (tx:Transaction) where tx.TransID in transactionIds and tx.amount > threshold
    return t.TermID, tx.amount, threshold
    '''
    def getFraudulantTransactionsPerTerminal(self):
        query = "MATCH "
        query += f"(t:Terminal) -[:On]- (tx:Transaction) where tx.datetime > '{str(datetime.datetime.now().date() - datetime.timedelta(days=31))}' "\
                f"with t, 0.2 * max(tx.amount) as threshold, collect(tx.TransID) as transactionIds "\
                f"Match (tx:Transaction) where tx.TransID in transactionIds and tx.amount > threshold "\
                f"return t.TermID, tx.amount, threshold"
        
        return self.run(query)
    
    '''pt.c
        match (c:Customer {id: 1})-[:Use*2..2]-(:Terminal)-[:Use]-(c1:Customer) where c <> c1 return c, c1

        #match (c1:Customer) -[:Use*1..3]- (c2:Customer) return c1, c2
    '''
    def getCocustomerOfCustomer(self, customerId:int, degree:int = 3):
        query = "MATCH "
        query += f"(c:Customer {{id: {customerId}}}) -[:Use*{degree - 1}..{degree - 1}]-(:Terminal)-[:Use]-(c1:Customer) "\
                f"where c <> c1 return c, c1 "
        
        return self.run(query)
    
    '''pt.d1
    match (t:Transaction)
    set t.period =
        case round(rand()*3) 
            when 0 then 'Morning'
            when 1 then 'Afternoon'
            when 2 then 'Evening'
            else 'Night'
        end
    set t.kindOfProduct=
        case round(rand()*4)
            when 0 then 'high-tech'
            when 1 then 'food'
            when 2 then 'clothing'
            when 3 then 'consumable'
            else 'other'
        end
    set t.feeling=  round(rand()*4)
    #Rand() restituisce un numero frazionale in [0:1] 
    '''
    def extendTransactions(self):
        query = "Match "
        query += f"(t:Transaction) set t.period = "\
                f"case round(rand()*3) "\
                f"when 0 then 'Morning' "\
                f"when 1 then 'Afternoon' "\
                f"when 2 then 'Evening' "\
                f"else 'Night' "\
                f"end "\
                f"set t.kindOfProduct = "\
                f"case round(rand()*4) "\
                f"when 0 then 'high-tech' "\
                f"when 1 then 'food' "\
                f"when 2 then 'clothing' "\
                f"when 3 then 'consumable' "\
                f"else 'other' "\
                f"end "\
                f"set t.feeling = round(rand()*4)"
        
        return self.run(query)

    '''pt.d2
    '''
    def extendCustomers():
        return