from neo4j import GraphDatabase
import datetime
from enum import Enum
import random
import time

# class TransactionPeriod (Enum):
#     morning = 'morning', 
#     afternoon = 'afternoon', 
#     evening = 'evening', 
#     night = 'night'

# class TransactionProductType (Enum):
#     hightech = 'high-tech', 
#     food = 'food', 
#     clothing = 'clothing', 
#     consumable = 'consumable', 
#     other = 'other'

def timerEvaluationDecorator(func):
    def wrapper(*args):
        start_time = time.time()
        result = func(*args)
        end_time = time.time()
        execution_time = end_time - start_time

        print(f"Function '{func.__name__}' took {execution_time:.4f} seconds!")

        return result
    
    return wrapper

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
    @timerEvaluationDecorator
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
    set tx.isFraudulent = true
    return t.TermID, tx.amount, threshold
    '''
    @timerEvaluationDecorator
    def getFraudulentTransactionsPerTerminal(self):
        query = "MATCH "
        query += f"(t:Terminal) -[:On]- (tx:Transaction) where tx.datetime > '{str(datetime.datetime.now().date() - datetime.timedelta(days=31))}' "\
                f"with t, 0.2 * max(tx.amount) as threshold, collect(tx.TransID) as transactionIds "\
                f"Match (tx:Transaction) where tx.TransID in transactionIds and tx.amount > threshold "\
                f"Set tx.isFraudulent = true "\
                f"return t.TermID, tx.amount, threshold"
        
        return self.run(query)
    
    '''pt.c
        match (c:Customer {CustomerID: 1})-[:Use*2..2]-(:Terminal)-[:Use]-(c1:Customer) where c <> c1 return c, c1
        #in case of huge datasets we need to add [limit n]
        #match (c1:Customer) -[:Use*1..3]- (c2:Customer) return c1, c2
    '''
    @timerEvaluationDecorator
    def getCocustomerOfCustomer(self, customerId:int, degree:int = 3):
        query = "MATCH "
        query += f"(c:Customer {{CustomerID: {customerId}}}) -[:Use*{degree - 1}..{degree - 1}]-(:Terminal)-[:Use]-(c1:Customer) "\
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
    set t.feeling=  round((rand()*4) +1)
    #Rand() restituisce un numero frazionale in [0:1] 
    '''
    @timerEvaluationDecorator
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
                f"set t.feeling = round((rand()*4) + 1)"
        
        return self.run(query)

    '''pt.d2
    match (c:Customer)-[:Invoke]-(tx:Transaction)-[:On]-(t:Terminal)-[:On]-(tx1:Transaction)-[:Invoke]-(c1:Customer) 
    with c, c1, t, collect(tx) as cTx, collect(tx1) as c1Tx1, avg(tx.feeling) as avgC, avg(tx1.feeling) as avgC1 
    where size(cTx) > 3 and size(c1Tx1) > 3 and abs(avgC - avgC1) < 1 and c <> c1 
    merge (c)-[:Buying_friends]->(c1)
    '''
    @timerEvaluationDecorator
    def extendCustomers(self):
        query = "Match "
        query += f"(c:Customer)-[:Invoke]-(tx:Transaction)-[:On]-(t:Terminal)-[:On]-(tx1:Transaction)-[:Invoke]-(c1:Customer) "\
                f"with c, c1, t, collect(tx) as cTx, collect(tx1) as c1Tx1, avg(tx.feeling) as avgC, avg(tx1.feeling) as avgC1 "\
                f"where size(cTx) > 3 and size(c1Tx1) > 3 and abs(avgC - avgC1) < 1 and c <> c1 "\
                f"merge (c)-[:Buying_friends]->(c1)"
        
        return self.run(query)
    
    '''pt.e
    match (trans:Transaction) with trans.period as period, count(trans) as numPeriodTrans, collect(trans.TransID) as periodTransIds
    match (t:Transaction) where t.TransID in periodTransIds and t.isFraudulent = true 
    return period, numPeriodTrans, toFloat(count(t))/toFloat(numPeriodTrans) as avgFraudulentTrans
    '''
    @timerEvaluationDecorator
    def getFraudulentTransPerPeriod(self, executeFraudulantTransMarker = False):

        if executeFraudulantTransMarker:
            #prepara le transactions fraudulente supponendo che getFraudulantTransactionsPerTerminal() non è stato chiamato:
            print(self.getFraudulentTransactionsPerTerminal())

        query = "Match "
        query += f"(trans:Transaction) with trans.period as period, count(trans) as numPeriodTrans, collect(trans.TransID) as periodTransIds "\
                f"match (t:Transaction) where t.TransID in periodTransIds and t.isFraudulent = true  "\
                f"return period, numPeriodTrans, toFloat(count(t))/toFloat(numPeriodTrans) as avgFraudulentTrans"
        
        return self.run(query)
    
    '''
    match (n) detach delete n
    '''
    
    def resetDB(self):
        query = "MATCH "
        query += f"(n) detach delete n"
        
        return self.run(query)
    
    def closeDriver(self):
        self._driver.close()