# 
# see https://lestermartin.wordpress.com/2023/10/19/viewing-astronauts-thru-windows-more-pystarburst-examples/
# 

import trino
from pystarburst import Session
from pystarburst import functions as F
from pystarburst.functions import col, lag, lead, row_number, round
from pystarburst.window import Window as W

db_parameters = {
    "host": "tXXXXXXXXXXe.trino.galaxy.starburst.io",
    "port": 443,
    "http_scheme": "https",
    "auth": trino.auth.BasicAuthentication("lXXXXXXXm/aXXXXXXXXXn", "<password>")
}
session = Session.builder.configs(db_parameters).create()


print("")
print("---------------------------")
print("Take a peek at a couple of astronauts missions")

a = session.table("sample.demo.astronauts")
a.show(10);


print("")
print("---------------------------")
print("What does the schema look like?")

#Get all column names and their types
for field in a.schema.fields:
    print(field.name +" , "+str(field.datatype))


print("")
print("---------------------------")
print("Apply some projection & filtering")

# identify the two astronauts we want to focus on
li = ["Nicollier, Claude", "Ross, Jerry L."]
twoAs = a.select("name", "nationality", \
				 "year_of_mission", "hours_mission") \
	.rename("year_of_mission", "m_yr") \
	.rename("hours_mission", "m_hrs") \
	.filter(a.name.isin(li)) \
	.sort("name", "m_yr")
twoAs.show(20)


print("")
print("---------------------------")
print("See how each mission compares with an ")
print("  OVERALL average across ALL missions")

# trim out the nation column
aDF = twoAs.drop("nationality")

aDF.withColumn("avg_all_m_hrs", F.avg("m_hrs").over()) \
	.sort("name", "m_yr").show(20)


print("")
print("---------------------------")
print("Have a window per person of all their rows")

# define the window specification
w2 = W.partition_by("name")

aDF.withColumn("tot_m_hrs", F.sum("m_hrs").over(w2)) \
	.sort("name", "m_yr").show(20)


print("")
print("---------------------------")
print("Multiple aggs for the same window")

# chain another withColumn method
aDF.withColumn("avg_m_hrs", F.avg("m_hrs").over(w2)) \
	.withColumn("tot_m_hrs", F.sum("m_hrs").over(w2)) \
	.sort("name", "m_yr").show(20)


print("")
print("---------------------------")
print("Another ex: multiple aggs for the same window")

# manipulate the window's agg value
aDF.withColumn("percent_of_tot", 
		round(aDF.m_hrs / F.sum("m_hrs").over(w2) * 100, 
			2)) \
	.withColumn("tot_m_hrs", F.sum("m_hrs").over(w2)) \
	.sort("name", "m_yr").show(20)


print("")
print("---------------------------")
print("Different windows by different partition_by's")

# define another window specification
w3 = W.partition_by("m_yr")

aDF.withColumn("tot_m_hrs", F.sum("m_hrs").over(w2)) \
	.withColumn("tot_m_yr_for_all", 
		F.count("m_yr").over(w3)) \
	.sort("name", "m_yr").show(20)


print("")
print("---------------------------")
print("Ordering window contents")

# define another window specification
w4 = W.partition_by("name").order_by("m_yr")

# row_number
aDF.withColumn("m_nbr", row_number().over(w4)) \
	.sort("name", "m_yr").show(20)

# lag and lead
aDF.withColumn("prev_m_hrs", 
		lag(aDF.m_hrs, 1).over(w4)) \
	.withColumn("next_m_hrs", 
		lead(aDF.m_hrs, 1).over(w4)) \
	.sort("name", "m_yr").show(20)


print("")
print("---------------------------")
print("Rolling windows")

w5 = W.partition_by("name").order_by("m_yr") \
	.rows_between(W.UNBOUNDED_PRECEDING, W.CURRENT_ROW)

aDF.withColumn("running_tot_m_hrs", 
		F.sum("m_hrs").over(w5)) \
	.sort("name", "m_yr").show(20)

w6 = W.partition_by("name").order_by("m_yr") \
	.rows_between(-2, W.CURRENT_ROW)
	
# rollling avg over curr rec and last two
aDF.withColumn("avg_this_and_last2", 
		round(F.avg("m_hrs").over(w6), 2)) \
	.sort("name", "m_yr").show(20)
		
	
print("")
print("---------------------------")
print("Sophisticated window ranges")

w7 = W.partition_by("name").order_by("m_yr") \
	.range_between(-2, W.CURRENT_ROW)
	
aDF.withColumn("avg_last2_YEARS", 
		round(F.avg("m_hrs").over(w7), 2)) \
	.withColumn("avg_last2_ROWS", 
		round(F.avg("m_hrs").over(w6), 2)) \
	.sort("name", "m_yr").show(20)


print("")
print("---------------------------")
print("Start with some SQL")

twoAs_fromSQL = session.sql(
    "SELECT name, "\
    "       year_of_mission AS m_yr, "\
    "       hours_mission AS m_hrs "\
    "  FROM sample.demo.astronauts "\
    " WHERE name IN ('Nicollier, Claude', "\
    "                'Ross, Jerry L.')")

w8 = W.partition_by("name")

twoAs_fromSQL.withColumn("tot_m_hrs", 
		F.sum("m_hrs").over(w2)) \
	.sort("name", "m_yr").show(20)


print("")
print("---------------------------")
print("Do it all with SQL")

ALL_fromSQL = session.sql(
    "SELECT name, "\
    "       year_of_mission AS m_yr, "\
    "       hours_mission AS m_hrs, "\
    "       SUM(hours_mission) "\
    "          OVER (PARTITION BY name) "\
    "       AS tot_m_hrs "\
    "  FROM sample.demo.astronauts "\
    " WHERE name IN ('Nicollier, Claude', "\
    "                'Ross, Jerry L.')"\
    " ORDER BY name, m_yr ")

ALL_fromSQL.show(20)