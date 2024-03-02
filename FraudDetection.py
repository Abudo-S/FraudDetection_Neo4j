from DataLoader import DataLoader
from DataGenerator import DataGenerator
from DataElaborator import DataElaborator
import pandas as pd
import datetime

'CUSTOMER_ID',
'x_customer_id', 'y_customer_id',
'mean_amount', 'std_amount',
'mean_nb_tx_per_day'

class Customer ():
    CUSTOMER_ID: int
    x_customer_id: float
    y_customer_id: float
    mean_amount: float
    std_amount: float
    mean_nb_tx_per_day: float
    available_terminals: list #terminalIds

    def __init__(self, customer, terminalIds):
        self.CUSTOMER_ID = customer['CUSTOMER_ID']
        self.x_customer_id= customer['x_customer_id']
        self.y_customer_id= customer['y_customer_id']
        self.mean_amount = customer['mean_amount']
        self.std_amount = customer['std_amount']
        self.mean_nb_tx_per_day = customer['mean_nb_tx_per_day']
        self.available_terminals = terminalIds

def generateAndImportDataSet(dl:DataLoader, dg: DataGenerator):

    try:
        customers = pd.DataFrame()
        while True:
            customers_partial = dg.generate_customer_profiles_table(100)
            if customers_partial is None: #se è stata superata la soglia di bits
                break
            customers = pd.concat([customers, customers_partial])
        
        dl.importCustomers(customers)

        terminals = pd.DataFrame()
        while True:
            terminals_partial = dg.generate_terminal_profiles_table(100)
            if terminals_partial is None: #se è stata superata la soglia di bits
                    break
            terminals = pd.concat([terminals, terminals_partial])

        dl.importTerminals(terminals)

        x_y_terminals = [terminals.iloc[terminalIdx][['x_terminal_id','y_terminal_id']].values.astype(float) for terminalIdx in range(len(terminals))]
        
        for customerIdx in range(len(customers)):
            customer = customers.iloc[customerIdx]
            terminalIds = dg.get_list_terminals_within_radius(customer, x_y_terminals, 0.002)
            
            #aggiungere i terminals per customer
            dl.importCustomerTerminals(customer, terminalIds)
                
            c_transactions = dg.generate_transactions_table(Customer(customer, terminalIds), "2024-02-01", 30)

            if c_transactions is None: #se è stata superata la soglia di bits
                break
            
            #aggiungere le transaction per customer
            dl.importCustomerTransactions(c_transactions)
    except:
        return False

    return True


if __name__ == "__main__":
    uri = "neo4j+s://8c0e259c.databases.neo4j.io"
    user = "neo4j"
    password = "RspDn6pEiaKrLCm9GuhD5dnCWGzqQC3Z05uoCvFVVJw"

    dl = DataLoader(uri, user, password)
    de = DataElaborator(uri, user, password)
    #de.resetDB()
    dg_50MB = DataGenerator(20000) #53000000
    dg_100MB = DataGenerator(106000000)
    dg_200MB = DataGenerator(212000000)

    # conn.run(f"CREATE (:Customer {{CUSTOMER_ID: 0.0, location: [54.88135039273247, 71.51893663724195], mean_amount: 62.262520726806166, std_amount: 31.131260363403083}})")
    # print()
    # xy_custom = np.array([50, 24])
    # xy_terminals = np.array([[5, 2], [1, 2], [20, 10]])
    # result = xy_custom - xy_terminals
    
    if generateAndImportDataSet(dl, dg_50MB):
        print("Dataset generato ed importato al DB con successo")
    else:
        print("Errore durante la generazione o l'imporazione del dataset!")


    result = de.getCustomerLastMonthLimit()
    print(result)

    result = de.getFraudulentTransactionsPerTerminal()
    print(result)
        
    #extend customers' relationships
    de.extendCustomers()

    #extend transactions' attributes
    de.extendTransactions()

    result = de.getFraudulentTransPerPeriod(False)
    print(result)
    
    de.closeDriver()

    print("completato")

'''
MATCH (c:Customer), (t:Terminal)
WHERE t.x - c.x < $distance_threshold and t.
CREATE (c)-[:CONNECTED_TO {distance: distance(c.x, c.y, t.x, t.y)}]->(t)
'''