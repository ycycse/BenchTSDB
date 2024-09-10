for i in 1 10 100 1000 # check check check !
do
	for (( j = 1 ; j <= 6; j++ ))
	do
		echo "Query Param=$i, loop=$j/6"
		ps -ef|grep 'postgres'|awk '{print $2}'|xargs kill -9
		sleep 60s

                echo 3 |sudo tee /proc/sys/vm/drop_caches 
                free -m
                sleep 2s

                sudo -u postgres /usr/lib/postgresql/12/bin/pg_ctl -D /data/dbms/postgres12/data/ > /data/dbms/postgres12/log/postgres.log start
                sleep 100s #这个不能太小，否则服务器还没启动完毕后面就查询了，调大是比较安全的
                
                cd /data/dbms/BenchTSDB
                java -jar /data/dbms/BenchTSDB/BenchTSDB-1.0-jar-with-dependencies.jar -r "/data/dbms/BenchTSDB/configs/timescaledb/TimescaleDB+MULTI_SERIES_ALIGN_QUERY_$i.properties" #note this should NOT be made background !!!!!!
        done
done

ps -ef|grep 'postgres'|awk '{print $2}'|xargs kill -9
sleep 60s

echo 3 |sudo tee /proc/sys/vm/drop_caches 
free -m

echo "ALL DONE!"
