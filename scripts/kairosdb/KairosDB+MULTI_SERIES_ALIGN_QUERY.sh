# bash KairosDB+MULTI_SERIES_ALIGN_QUERY.sh > KairosDB+MULTI_SERIES_ALIGN_QUERY.txt

for i in 1 10 100 1000 # check check check !
do
	for (( j = 1 ; j <= 6; j++ ))
	do
		echo "Query Param=$i, loop=$j/6"
                #停止服务器
                echo stop-cassandra...
		ps -ef|grep '[o]rg.apache.cassandra.service.CassandraDaemon'|awk '{print $2}'|xargs kill -9
		sleep 60s
		ps -ef | grep cassandra
		
		echo stop-kairosdb...
		ps -ef|grep "[o]rg.kairosdb.core.Main -c start -p"|awk '{print $2}' | xargs kill -9
		sleep 60s
		ps -ef | grep kairosdb

                echo 3 |sudo tee /proc/sys/vm/drop_caches 
                free -m
                sleep 2s

                #开启服务器
                echo start-cassandra...
		nohup /data/dbms/apache-cassandra-3.11.2/bin/cassandra -f  -R > /dev/null 2>&1 &  #-R必须加上，-f应该时前台运行的意思
		sleep 120s #这个不能太小，否则服务器还没启动完毕后面就查询了，调大是比较安全的

		echo start-kairosdb...
		/data/dbms/kairosdb/bin/kairosdb.sh  start  #不用nohup &是因为start已经是后台运行了，run是前台
		sleep 60s #这个不能太小，否则服务器还没启动完毕后面就查询了，调大是比较安全的
                
                cd /data/dbms/BenchTSDB
                java -jar /data/dbms/BenchTSDB/BenchTSDB-1.0-jar-with-dependencies.jar -r "/data/dbms/BenchTSDB/configs/kairosdb/KairosDB+MULTI_SERIES_ALIGN_QUERY_$i.properties" #note this should NOT be made background !!!!!!
        done
done

#停止服务器
echo stop-cassandra...
ps -ef|grep '[o]rg.apache.cassandra.service.CassandraDaemon'|awk '{print $2}'|xargs kill -9
sleep 60s

echo stop-kairosdb...
ps -ef|grep "[o]rg.kairosdb.core.Main -c start -p"|awk '{print $2}' | xargs kill -9
sleep 60s

echo 3 |sudo tee /proc/sys/vm/drop_caches 
free -m

echo "ALL DONE!"
