package com.jasongj.spark.reader;

import com.google.common.base.Preconditions;
import com.jasongj.spark.model.Bucket;
import com.jasongj.spark.model.TableMetaData;
import com.jasongj.spark.model.Tuple;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.parquet.example.data.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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

    public BucketReaderIterator(Configuration hadoopConfiguration, TableMetaData tableMetaData, Integer bucketID){
        this.fieldTypes = tableMetaData.getFields().stream().map(FieldSchema::getType).collect(Collectors.toList());
        this.keyIndex = tableMetaData.getBucketColumns();
    }

    @Override
    public boolean hasNext() {
        if(this.recordReader == null) {
            return false;
        }
        return preFetch() != null;
    }

    public abstract Tuple preFetch();

    @Override
    public Tuple next() {
        if(this.recordReader == null) {
            return null;
        }
        if(this.header != null || preFetch() != null) {
            Tuple tuple = this.header;
            this.header = null;
            return tuple;
        } else {
            return null;
        }
    }

    @Override
    public void remove() {}

}
