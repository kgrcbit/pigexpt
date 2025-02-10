a = LOAD '/BusiestRoutes/input/exercise2.csv' USING PigStorage(',') AS (Year:int,Month:int,DayofMonth:int,DayOfWeek:int,DepTime:int,CRSDepTime:int,ArrTime:int,CRSArrTime:int,UniqueCarrier:chararray,FlightNum:int,TailNum:int,ActualElapsedTime:int,CRSElapsedTime:int,AirTime:int,ArrDelay:int,DepDelay:int,Origin:chararray,Dest:chararray,Distance:int,TaxiIn:int,TaxiOut:int,Cancelled:int,CancellationCode:chararray,Diverted:int,CarrierDelay:int,WeatherDelay:int,NASDelay:int,SecurityDelay:int,LateAircraftDelay:int);                            /* loading the input file from hdfs into pig relation */ 
d = GROUP a BY (Origin, Dest);                                                               /* Grouping with origin and destination */
count_routes = FOREACH d GENERATE group, COUNT(a) AS count1;                                /* highest freuqncy between routes */
sorted = ORDER count_routes BY count1 DESC;                                                 /* Orders the routes between origin and destination in the descending */
  
STORE sorted INTO '/BusiestRoutes/output/traffic';                                  /* Stores the results to the S3 bucket in the Output folder by filename traffic */
