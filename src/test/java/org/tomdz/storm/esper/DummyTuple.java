package org.tomdz.storm.esper;


import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

public class DummyTuple implements Tuple {

    private final String src;
    private final String streamId;
    private final List<String> keys = Lists.newArrayList();
    private final List<Object> values = Lists.newArrayList();


    public DummyTuple(String src, String streamId, Map<String, Object> data) {
        this.src = src;
        this.streamId = streamId;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            keys.add(entry.getKey());
            values.add(entry.getValue());
        }
    }

    @Override
    public String getSourceComponent() {
        return src;
    }

    @Override
    public String getSourceStreamId() {
        return streamId;
    }

    @Override
    public Fields getFields() {
        return new Fields(keys);
    }

    @Override
    public Object getValue(int index) {
        return values.get(index);
    }

    @Override
    public int size() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int fieldIndex(String s) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean contains(String s) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }


    @Override
    public String getString(int i) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Integer getInteger(int i) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Long getLong(int i) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Boolean getBoolean(int i) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Short getShort(int i) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Byte getByte(int i) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Double getDouble(int i) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Float getFloat(int i) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public byte[] getBinary(int i) {
        return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Object getValueByField(String s) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getStringByField(String s) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Integer getIntegerByField(String s) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Long getLongByField(String s) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Boolean getBooleanByField(String s) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Short getShortByField(String s) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Byte getByteByField(String s) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Double getDoubleByField(String s) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Float getFloatByField(String s) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public byte[] getBinaryByField(String s) {
        return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<Object> getValues() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<Object> select(Fields strings) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public GlobalStreamId getSourceGlobalStreamid() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }


    @Override
    public int getSourceTask() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public MessageId getMessageId() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
