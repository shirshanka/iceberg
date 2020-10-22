package org.apache.iceberg.spark.source;

import java.util.Objects;

public class SimpleRecord2 {
    private Integer id;
    private String data;
    private long ts;

    public SimpleRecord2(Integer id, String data, long ts) {
        this.id = id;
        this.data = data;
        this.ts = ts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleRecord2 that = (SimpleRecord2) o;
        return ts == that.ts &&
                Objects.equals(id, that.id) &&
                Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, data, ts);
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }
}
