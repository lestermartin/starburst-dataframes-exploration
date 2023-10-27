#
# See https://lestermartin.wordpress.com/2023/10/27/ibis-trino-dataframe-api-part-deux/
#

import os
import ibis

ibis.options.interactive = True

user = "lXXXXXXXm/aXXXXXXXXn"
password = "<password>"
host = "lXXXXXXXXXXXo"
port = "443"
catalog = "tpch"
schema = "tiny"

con = ibis.trino.connect(
    user=user, password=password, host=host, port=port, 
    database=catalog, schema=schema
)

tiny_region = con.table("region")
print(tiny_region[0:5])

# Select a full table
custDF = con.table("customer")
print(custDF.head(10))

# Project fewer columns
projectedDF = custDF.select("name", "acctbal", "nationkey")
print(projectedDF.head(10))

# Filter out some rows
filteredDF = projectedDF.filter(projectedDF["acctbal"] > 9900.0)
print(filteredDF.head(100))

# Grab new table, drop 2 cols, and rename 2 others
nationDF = con.table("nation") \
			.drop("regionkey", "comment") \
			.rename(
				dict(
					nation_name="name",
					n_nationkey="nationkey"
				)
			)
print(nationDF.head(10))

# Join the tables
joinedDF = filteredDF.join(nationDF, 
	filteredDF.nationkey == nationDF.n_nationkey)
print(joinedDF.head(10))

# Drop a couple of the columns
projectedJoinDF = joinedDF.drop("nationkey", "n_nationkey")
print(projectedJoinDF.head(10))

# Sort the results
orderedDF = projectedJoinDF.order_by([ibis.desc("acctbal")])
print(orderedDF.head(10))

# Do it all again, but chain the methods this time
custDF = con.table("customer") \
            .select("name", "acctbal", "nationkey") \
            .filter(projectedDF["acctbal"] > 9900.0)
apiSQL = custDF.join(nationDF, 
	custDF.nationkey == nationDF.n_nationkey) \
            .drop("nationkey", "n_nationkey") \
            .order_by([ibis.desc("acctbal")])
print(apiSQL.head(10))

