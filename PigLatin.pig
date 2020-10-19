# Open the Pig interface Grunt by typing in 'Pig'

# CREATE YOUR SCRIPT and DEFINE RELATIONS
# Define a relation named 'truck_events' that represents all the truck events

  truck_events = LOAD '/user/rlunett/truck_event_text_partition.csv' USING PigStorage(',');

# View Relation

  DESCRIBE truck_events;

# DEFINE A RELATION WITH A SCHEMA

  truck_events = LOAD '/user/rlunett/truck_event_text_partition.csv' USING PigStorage(',')
  AS (driverId:int, truckId:int, eventTime:chararray, eventType:chararray, longitude:double
  , latitude:double, eventKey:chararray, correlationId:long, driverName:chararray
  , routeId:long,routeName:chararray,eventDate:chararray);

# DEFINE A NEW RELATION FROM AN EXISTING RELATION
# Define the following truck_events_subset relation, which is a collection of 100 entries (arbitrarily selected) from the truck_events relation.

  truck_events_subset = LIMIT truck_events 100;

# View Relation

  DESCRIBE truck_events_subset;

# VIEW THE DATA
# Initiate the MapReduce job to execute. The output should be 100 entries from the contents of the truck_events_text_partition.csv file.

  DUMP truck_events_subset;
  
# SELECT SPECIFIC COLUMNS FROM A RELATION
# Pig Data Transformation - Define a new relation based on the fields of an existing relation using the FOREACH command.

  DESCRIBE truck_events;
  
  specific_columns = FOREACH truck_events_subset GENERATE driverId, eventTime, eventType;
  
  DESCRIBE specific_columns;
  
# STORE RELATIONSHIP DATA INTO A HDFS FILE
# Use the STORE command to output a relation into a new file in HDFS. 
# Output the specific_columns relation to a folder named output/specific_columns

  STORE specific_columns INTO 'output/specific_columns' USING PigStorage(',');
  
# ***REFER TO OUTPUT.SH FILE IN REPO*** #
hdfs dfs -cat output/specific_columns/part-r-00000 | head

# PERFORM A JOIN BETWEEN 2 RELATIONS
# perform a join on two driver statistics data sets: truck_event_text_partition.csv and the driver.csv files. 
# Drivers.csv has all the details for the driver like driverId, name, ssn, location, etc.

# Create a new Pig script named "Pig-Join"
# Define a new relation named drivers then join truck_events and drivers by driverId and describe the schema of the new relation join_data.

  drivers = LOAD '/user/rlunett/drivers.csv' USING PigStorage(',') 
  AS (driverId:int, name:chararray, ssn:chararray, location:chararray, certified:chararray, wage_plan:chararray);
  
  join_data = JOIN truck_events BY (driverId), drivers BY (driverId);
  
  DESCRIBE join_data;
  
# SORT THE DATA USING “ORDER BY”
  
  ordered_data = ORDER drivers BY name asc;
  DUMP ordered_data;
  
# FILTER AND GROUP THE DATA USING “GROUP BY”
# Create a new Pig script named "Pig-Group". 
# Group the truck_events relation by the driverId for the eventType which are not ‘Normal’.
  
  filtered_events = FILTER truck_events BY NOT (eventType MATCHES 'Normal');
  
  grouped_events = GROUP filtered_events BY driverId;
  
  DESCRIBE grouped_events;
  
  DUMP grouped_events;
