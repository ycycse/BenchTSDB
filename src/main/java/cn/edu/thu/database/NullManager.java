package cn.edu.thu.database;

import cn.edu.thu.common.Record;
import cn.edu.thu.common.Schema;
import java.util.List;

public class NullManager implements IDataBaseManager {

  public NullManager() {

  }

  @Override
  public long insertBatch(List<Record> records, Schema schema) {
    return 0;
  }

  @Override
  public void initServer() {

  }

  @Override
  public void initClient() {

  }

  @Override
  public long query() {
    return 0;
  }

  @Override
  public long flush() {
    return 0;
  }

  @Override
  public long close() {
    return 0;
  }
}
