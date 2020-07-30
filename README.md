
# Web Log Challenge in Spark
Web log challenge data analysis using Spark 2.4.3

- - -
## Processing & Analytical goals:
1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
   https://en.wikipedia.org/wiki/Session_(web_analytics)

2. Determine the average session time

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

4. Find the most engaged users, ie the IPs with the longest session times


- - -
## How to execute:
The implementation is done by local mode, so you can run in your local or any IDE.  
Before deploy to production, remember to change spark session to cluster mode and base on requirement to change the output path and format (parquet/ avro/ json).
