package cn.edu.thu.database.kairosdb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * example:
 * <p>
 * [{ "name": "archive.file.tracked", "timestamp": 1349109376, "type": "long", "value": 123,
 * "tags":{"host":"test"} }, { "name": "archive.file.search", "timestamp": 999, "type": "double",
 * "value": 32.1, "tags":{"host":"test"} }]
 */
public class KairosDBPoint implements Serializable {

  private static final long serialVersionUID = 1L;
  private String name;
  private Long timestamp;
  private String type;
  private Object value;
  private Map<String, String> tags = new HashMap<>();
  private List<List<Object>> datapoints;

  public void addDatapoints(List<Object> point) {
    if (datapoints == null) {
      datapoints = new ArrayList<>();
    }
    datapoints.add(point);
  }

  public List<List<Object>> getDatapoints() {
    return datapoints;
  }

  public String getName() {
    return name;
  }

  public void setName(String metric) {
    this.name = metric;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

}
