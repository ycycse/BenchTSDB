datasetArray=("InfluxDB+SINGLE_SERIES_RAW_QUERY" "InfluxDB+SINGLE_SERIES_COUNT_QUERY" "InfluxDB+SINGLE_SERIES_DOWNSAMPLING_QUERY");
for value in ${datasetArray[@]};
do

./../tool.sh QUERY_PARAM 100000 $value.properties 
cp $value.properties ${value}_100000.properties

./../tool.sh QUERY_PARAM 10000000 $value.properties 
cp $value.properties ${value}_10000000.properties

done
