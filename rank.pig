input2 = LOAD '/flightsdata/input/exercise2.csv' USING PigStorage(',') AS (Year:int,Month:int,DayofMonth:int,DayOfWeek:int,DepTime:int,CRSDepTime:int,ArrTime:int,CRSArrTime:int,UniqueCarrier:chararray,FlightNum:int,TailNum:int,ActualElapsedTime:int,CRSElapsedTime:int,AirTime:int,ArrDelay:int,DepDelay:int,Origin:chararray,Dest:chararray,Distance:int,TaxiIn:int,TaxiOut:int,Cancelled:int,CancellationCode:chararray,Diverted:int,CarrierDelay:int,WeatherDelay:int,NASDelay:int,SecurityDelay:int,LateAircraftDelay:int);                                           /* loading the input file from the hdfs into pig relation */

ontime = FILTER input2 BY DepDelay < 15 AND ArrDelay <15;                                              /* Finding the on-time flights that have departure delay and arrival delay both < 15 minutes */
 
b_mod = FOREACH ontime GENERATE UniqueCarrier,FlightNum;                                                   /* This projections the two columns UniqueCarrier and FlightNum from input1 relation */
group_carriers = GROUP b_mod BY UniqueCarrier;                                                             /* Group by UniqueCarrier */
group_ontime = GROUP b_mod ALL;                                                                            /* Group All operator */
ontime_count = FOREACH group_ontime GENERATE 'same' AS key,(DOUBLE) COUNT(b_mod) AS count2;                /* This will generate the number of on-time flights */
DESCRIBE ontime_count;                                                                                     /* This will display the schema for the ontime_count relation */
car_count = FOREACH group_carriers GENERATE 'same' AS key, group,(DOUBLE) COUNT(b_mod) AS count1;          /* This will generate the number of on-time flights by carrier */
sorted = ORDER car_count BY count1 DESC;                                                                   /* Ordering the car_count relation by the count in a descending order */
rank_1= RANK sorted;                                                                                       /* This will rank the sorted relation */
DESCRIBE rank_1;                                                                                           /* This will show the schema for the rank_1 relation */ 
z = JOIN ontime_count BY key,rank_1 BY key;                                                                /* Joining the two relations by key */
result = FOREACH z GENERATE rank_1::rank_sorted, rank_1::group,rank_1::count1, (rank_1::count1/ontime_count::count2);   
                                                                                          /* This will generate the portions of the on-time flights by ranked carrier to the total on-time flights */
STORE result INTO 'flightsdata/Output/RankResult';                                      /* This will store the results to S3 Bucket in the Output folder witht the file name RankResult  */

