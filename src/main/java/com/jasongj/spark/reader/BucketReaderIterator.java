package com.jasongj.spark.reader;

import com.jasongj.spark.model.TableMetaData;
import com.jasongj.spark.model.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.mapreduce.RecordReader;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public abstract class BucketReaderIterator implements Iterator<Tuple> {
    protected RecordReader recordReader;
    protected Tuple header;
    protected List<String> fieldTypes;
    protected List<Integer> keyIndex;
    protected TableMetaData tableMetaData;

    public BucketReaderIterator(Configuration hadoopConfiguration, TableMetaData tableMetaData, Integer bucketID){
        this.fieldTypes = tableMetaData.getFields().stream().map(FieldSchema::getType).collect(Collectors.toList());
        this.keyIndex = tableMetaData.getBucketColumns();
        this.tableMetaData = tableMetaData;
    }

    @Override
    public boolean hasNext() {
        /*if(this.recordReader == null) {
            return false;
        }*/
        return fetchTuple() != null;
    }

    public abstract Tuple fetchTuple();

    @Override
    public Tuple next() {
        /*if(this.recordReader == null) {
            return null;
        }*/
        if(this.header != null || fetchTuple() != null) {
            Tuple tuple = this.header;
            this.header = null;
            return tuple;
        } else {
            return null;
        }
    }

    @Override
    public void remove() {

    }

}
