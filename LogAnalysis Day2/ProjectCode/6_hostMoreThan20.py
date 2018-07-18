#------- Frequent Hosts --------------
# Get the list of hosts who have visited website atleast 20 times
hostCountPairTuple = parsed_logs.map(lambda log: (log.host, 1))# Host, 1 key value pairs

hostSum = hostCountPairTuple.reduceByKey(lambda a, b : a + b) # Each host, total number of visits

hostMoreThan20 = hostSum.filter(lambda s: s[1] > 20) # Condition for more than 20

hostMoreThan20.saveAsTextFile("sparkProject/6_hostMoreThan20")

sc.stop()