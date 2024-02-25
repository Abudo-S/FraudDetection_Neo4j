from DataLoader import DataLoader
from DataGenerator import DataGenerator
import pandas as pd

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


if __name__ == "__main__":
    conn = DataLoader("neo4j+s://8c0e259c.databases.neo4j.io", "neo4j", "RspDn6pEiaKrLCm9GuhD5dnCWGzqQC3Z05uoCvFVVJw")
    dg_50MB = DataGenerator(53000000)
    # dg_100MB = DataGenerator(106000000)
    # dg_200MB = DataGenerator(212000000)
    # conn.run(f"CREATE (:Customer {{CUSTOMER_ID: 0.0, location: [54.88135039273247, 71.51893663724195], mean_amount: 62.262520726806166, std_amount: 31.131260363403083}})")
    # print()
    # xy_custom = np.array([50, 24])
    # xy_terminals = np.array([[5, 2], [1, 2], [20, 10]])
    # result = xy_custom - xy_terminals

    customers = dg_50MB.generate_customer_profiles_table(100)
    conn.importCustomers(customers)

    terminals = dg_50MB.generate_terminal_profiles_table(100)
    conn.importTerminals(terminals)

    x_y_terminals = [terminals.iloc[terminalIdx][['x_terminal_id','y_terminal_id']].values.astype(float) for terminalIdx in range(len(terminals))]

    for customerIdx in range(len(customers)):
        customer = customers.iloc[customerIdx]
        terminalIds = dg_50MB.get_list_terminals_within_radius(customer, x_y_terminals, 0.2)

        #aggiungere i terminals per customer
        conn.importCustomerTerminals(customer, terminalIds)
        
        #aggiungere le transaction per customer
        c_transactions = dg_50MB.generate_transactions_table(Customer(customer, terminalIds))
        conn.importCustomerTransactions(c_transactions)

    print("completato")
    

    '''
    MATCH (c:Customer), (t:Terminal)
WHERE distance(c.x, c.y, t.x, t.y) < $distance_threshold
CREATE (c)-[:CONNECTED_TO {distance: distance(c.x, c.y, t.x, t.y)}]->(t)
'''