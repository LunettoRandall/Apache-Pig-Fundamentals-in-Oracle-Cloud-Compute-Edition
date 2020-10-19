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
  

  
  


