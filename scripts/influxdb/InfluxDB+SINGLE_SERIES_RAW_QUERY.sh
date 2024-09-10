# bash xxx.sh > xxx.txt

#for i in 1 100 10000 100000 1000000
#for i in 1 10000 1000000 100000000
for i in 10000 100000 1000000 10000000 100000000
do
	for (( j = 1 ; j <= 6; j++ ))
	do
		echo "Query Param=$i, loop=$j/6"
                #停止服务器
                ps -ef|grep 'influxd'|awk '{print $2}'|xargs kill -9
		sleep 60s

                echo 3 |sudo tee /proc/sys/vm/drop_caches 
                free -m
                sleep 2s

                #开启服务器
                cd /data/dbms/influxdb1.8.10/influxdb-1.8.10-1/usr/bin
                nohup ./influxd -config /data/dbms/influxdb1.8.10/influxdb-1.8.10-1/etc/influxdb/influxdb.conf &
                sleep 120s # you really need to wait that long
                
                cd /data/dbms/BenchTSDB
                java -jar /data/dbms/BenchTSDB/BenchTSDB-1.0-jar-with-dependencies.jar -r "/data/dbms/BenchTSDB/configs/influxdb/InfluxDB+SINGLE_SERIES_RAW_QUERY_$i.properties" #note this should NOT be made background !!!!!!
        done
done

#停止服务器
ps -ef|grep 'influxd'|awk '{print $2}'|xargs kill -9
sleep 60s

echo 3 |sudo tee /proc/sys/vm/drop_caches 
free -m

echo "ALL DONE!"
