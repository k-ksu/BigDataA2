from cassandra.cluster import Cluster

print("hello app")

cluster = Cluster(["cassandra-server"])

session = cluster.connect()

rows = session.execute("DESC keyspaces")
for row in rows:
    print(row)
