# 
# see https://lestermartin.wordpress.com/2023/09/12/pystarburst-the-dataframe-api/ 
# 

import trino
from pystarburst import Session
from pystarburst import functions as f
from pystarburst.functions import col

db_parameters = {
    "host": "tXXXXXXXXXXe.trino.galaxy.starburst.io",
    "port": 443,
    "http_scheme": "https",
    "auth": trino.auth.BasicAuthentication("lXXXXXXXm/aXXXXXXXXXn", "<password>")
}
session = Session.builder.configs(db_parameters).create()

dfSQL = session.sql("SELECT c.name, c.acctbal, n.name "\
                    "  FROM tpch.tiny.customer c "\
                    "  JOIN tpch.tiny.nation n "\
                    "    ON c.nationkey = n.nationkey "\
                    " WHERE c.acctbal > 9900.0 "\
                    " ORDER BY c.acctbal DESC ")
dfSQL.show()

custDF = session.table("tpch.tiny.customer")
custDF.show()

projectedDF = custDF.select(custDF.name, custDF.acctbal, custDF.nationkey)
projectedDF.show()

filteredDF = projectedDF.filter(projectedDF.acctbal > 9900.0)
filteredDF.show()

nationDF = session.table("tpch.tiny.nation") \
                  .drop("regionkey", "comment") \
                  .rename("name", "nation_name") \
                  .rename("nationkey", "n_nationkey")
nationDF.show()

joinedDF = filteredDF.join(nationDF, filteredDF.nationkey == nationDF.n_nationkey)
joinedDF.show()

projectedJoinDF = joinedDF.drop("nationkey").drop("n_nationkey")
projectedJoinDF.show()

orderedDF = projectedJoinDF.sort(col("acctbal"), ascending=False)
orderedDF.show()


apiSQL = session.table("tpch.tiny.customer") \
                .select(custDF.name, custDF.acctbal, custDF.nationkey) \
                .filter(projectedDF.acctbal > 9900.0) \
                .join(nationDF, filteredDF.nationkey == nationDF.n_nationkey) \
                .drop("nationkey").drop("n_nationkey") \
                .sort(col("acctbal"), ascending=False)
apiSQL.show()