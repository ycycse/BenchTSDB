package cn.edu.thu.database;

import cn.edu.thu.common.Record;

import cn.edu.thu.common.Schema;
import java.util.List;

public interface IDataBaseManager {

    /**
     * @return time cost in ns
     */
    long insertBatch(List<Record> records, Schema schema);


    /**
     * init server once in main thread
     * may drop table, create necessary schema
     * no need to close the manager
     */
    void initServer();


    /**
     * init each client in child thread
     */
    void initClient();


    long query();

    /**
     * @return time cost in ns
     */
    long flush();

    /**
     * @return time cost in ns
     */
    long close();

}
