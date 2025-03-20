from cassandra.cluster import Cluster
import pandas as pd

# Connexion à Cassandra
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect('mykeyspace')  # Remplacez par votre keyspace

# Exécuter une requête CQL
query = "SELECT * FROM table_projet_avenir;"
rows = session.execute(query)

# Convertir les résultats en DataFrame pandas
df = pd.DataFrame(list(rows))

# Compter le nombre de lignes extraites
row_count = len(df)
print(f"Nombre de lignes extraites depuis Cassandra : {row_count}")

# Exporter en CSV
df.to_csv('cassandra_data.csv', index=False)
print("Données exportées avec succès dans 'cassandra_data.csv'.")
