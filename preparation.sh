# Download Dataset 
wget -O driver_data.zip https://raw.githubusercontent.com/hortonworks/data-tutorials/master/tutorials/hdp/beginners-guide-to-apache-pig/assets/driver_data.zip?raw=true

unzip driver_data.zip

# Upload csv files 
hdfs dfs -put drivers.csv .

hdfs dfs -put truck_event_text_partition.csv
