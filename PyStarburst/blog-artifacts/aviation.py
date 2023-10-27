# 
# see https://lestermartin.wordpress.com/2023/10/02/pystarburst-analytics-examples-querying-aviation-data-part-deux/
# 

import trino
from pystarburst import Session
from pystarburst import functions as f
from pystarburst.functions import col, lag, round, row_number
from pystarburst.window import Window

db_parameters = {
    "host": "tXXXXXXXXXXe.trino.galaxy.starburst.io",
    "port": 443,
    "http_scheme": "https",
    "auth": trino.auth.BasicAuthentication("lXXXXXXXm/aXXXXXXXXXn", "<password>")
}
session = Session.builder.configs(db_parameters).create()


print("")
print("Q1 ---------------------------")
print("How many rows in the flight table?")

allFs = session.table("mycloud.aviation.raw_flight")
print(allFs.count())


print("")
print("Q2 ---------------------------")
print("What country are most of the airports")
print("  located in?")

# get the whole table, aggregate & sort
mostAs = session \
    .table("mycloud.aviation.raw_airport") \
	.group_by("country").count() \
	.sort("count", ascending=False)
mostAs.show(1)


print("")
print("Q3 ---------------------------")
print("What are the top 5 airline codes with ")
print("  the most number of flights?")

# get the whole table, aggregate & sort
mostFs = session \
    .table("mycloud.aviation.raw_flight") \
	.group_by("unique_carrier").count() \
	.rename("unique_carrier", "carr") \
	.sort("count", ascending=False)
mostFs.show(5)


print("")
print("Q4 ---------------------------")
print("Same question, but show the airline ") 
print("  carrier's name.")

# get all of the carriers
allCs = session.table("mycloud.aviation.raw_carrier")

# repurpose mostFs from above (or chain on it) 
#   to join the 2 DFs and sort the results that
#   have already been grouped
top5CarrNm = mostFs \
    .join(allCs, mostFs.carr == allCs.code) \
    .drop("code") \
	.sort("count", ascending=False)
top5CarrNm.show(5, 30)


print("")
print("Q5 ---------------------------")
print("What are the most common airplane models ") 
print("  for flights over 1500 miles?")

# trimFs are flights projected & filtered
trimFs = session.table("mycloud.aviation.raw_flight") \
	.rename("tail_number", "tNbr") \
	.select("tNbr", "distance") \
	.filter(col("distance") > 1500) 

# trimPs are planes table projected & filtered
trimPs = session.table("mycloud.aviation.raw_plane") \
	.select("tail_number", "model") \
	.filter("model is not null")

# join, group & sort
q5Answer = trimFs \
	.join(trimPs, trimFs.tNbr == trimPs.tail_number) \
	.drop("tail_number") \
	.group_by("model").count() \
	.sort("count", ascending=False)	
q5Answer.show()


print("")
print("Q6 ---------------------------")
print("What is the month over month percentage ")
print("  change of number of flights departing ")
print("  from each airport?")

# temp DF or counts for each originating airport 
#   by month
aggFlights = session.table("mycloud.aviation.raw_flight") \
	.select("origination", "month") \
	.rename("origination", "orig") \
	.group_by("orig", "month").count() \
	.rename("count", "num_fs")

# define a window specification
w1 = Window.partition_by("orig").order_by("month")

# add col to grab the prior row's nbr flights
changeFlights = aggFlights \
	.withColumn("num_fs_b4", \
		lag("num_fs",1).over(w1))
	
# add col for the percentage change
q6Answer = changeFlights \
	.withColumn("perc_chg", \
		round((1.0 * (col("num_fs") - col("num_fs_b4")) / \
		      (1.0 * col("num_fs_b4"))), 1))
q6Answer.show()


print("")
print("Q7 ---------------------------")
print("Determine the top 3 routes departing from ")
print("  each airport. ")

# determine counts from orig>dest pairs
popularRoutes = session \
	.table("mycloud.aviation.raw_flight") \
	.rename("origination", "orig") \
	.rename("destination", "dest") \
	.group_by("orig", "dest").count() \
	.rename("count", "num_fs")

# define a window specification
w2 = Window.partition_by("orig") \
	.order_by(col("num_fs").desc())

# add col to put the curr row's ranking in
rankedRoutes = popularRoutes \
	.withColumn("rank", \
		row_number().over(w2))

# just show up to 3 for each orig airport
q7Answer = rankedRoutes \
	.filter(col("rank") <= 3) \
	.sort("orig", "rank")
q7Answer.show(17);