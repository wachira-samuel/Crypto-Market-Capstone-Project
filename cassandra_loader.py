from cassandra.cluster import Cluster

# Connect Cassandra
cluster = Cluster(["127.0.0.1"])

session = cluster.connect()

# Create keyspace
session.execute("""
CREATE KEYSPACE IF NOT EXISTS crypto
WITH replication = {
    'class':'SimpleStrategy',
    'replication_factor':1
}
""")

# Use keyspace
session.set_keyspace("crypto")

# Create table
session.execute("""
CREATE TABLE IF NOT EXISTS processed_prices (
    symbol TEXT PRIMARY KEY,
    avg_price DOUBLE
)
""")

# Insert sample data
session.execute("""
INSERT INTO processed_prices (symbol, avg_price)
VALUES ('BTCUSDT', 65000)
""")

print("Data inserted into Cassandra")
