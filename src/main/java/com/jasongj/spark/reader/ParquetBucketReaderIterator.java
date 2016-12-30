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
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
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
public class ParquetBucketReaderIterator extends BucketReaderIterator {
    private static final Logger LOG = LoggerFactory.getLogger(ParquetBucketReaderIterator.class);

    public ParquetBucketReaderIterator(Configuration hadoopConfiguration, TableMetaData tableMetaData, Integer bucketID) {
        super(hadoopConfiguration, tableMetaData, bucketID);
        TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(hadoopConfiguration, new TaskAttemptID());
        Bucket bucket = tableMetaData.getBuckets().get(bucketID);
        this.fieldTypes = tableMetaData.getFields().stream().map(FieldSchema::getType).collect(Collectors.toList());
        this.keyIndex = tableMetaData.getBucketColumns();
        FileSplit fileSplit = new FileSplit(new Path(bucket.getUri()), 0, bucket.getLength(), null);

        ParquetInputFormat<Group> parquetInputFormat = new ParquetInputFormat<Group>();

        try {
            this.recordReader = parquetInputFormat.createRecordReader(fileSplit, taskAttemptContext);
        } catch (IOException | InterruptedException ex) {
            this.recordReader = null;
            throw new RuntimeException("Construct BucketReaderIterator failed", ex);
        }
    }

    public Tuple preFetch() {
        try {
            if(this.header != null) {
                return this.header;
            }
            if(!recordReader.nextKeyValue()) {
                return null;
            }
            Group group = (Group)recordReader.getCurrentValue();
            List<Object> data = convertDataWithType(group, this.fieldTypes);
            List<Object> keys = keyIndex.stream().map((Integer index) -> data.get(index)).collect(Collectors.toList());
            this.header = new Tuple(keys, data);
            return this.header;
        } catch (IOException | InterruptedException  ex) {
            LOG.warn("Fetch tuple failed", ex);
            return null;
        }
    }

    private List<Object> convertDataWithType(Group group, List<String> fieldTypes) {
        Preconditions.checkNotNull(group);
        Preconditions.checkNotNull(fieldTypes);
        Preconditions.checkArgument(group.getType().getFieldCount() == fieldTypes.size());
        List<Object> data = new ArrayList<Object>();
        for(int i = 0; i < group.getType().getFieldCount(); i++) {
            String type = fieldTypes.get(i).toLowerCase();
            if(group.getFieldRepetitionCount(i) == 0) {
                data.add(null);
            } else {
                switch (type) {
                    case "string" : data.add(group.getValueToString(i, 0)); break;
                    case "bigdecimal": data.add(group.getDouble(i, 0)); break;
                    case "int": data.add(group.getInteger(i, 0)); break;
                    case "long": data.add(group.getLong(i, 0)); break;
                    case "double": data.add(group.getDouble(i, 0)); break;
                    case "float": data.add(group.getFloat(i, 0)); break;
                    case "boolean": data.add(group.getBoolean(i, 0)); break;
                    case "date": data.add(group.getValueToString(i, 0)); break;
                    case "timestamp": data.add(group.getValueToString(i, 0)); break;
                    case "short": data.add(group.getInteger(i, 0)); break;
                    case "byte": data.add(group.getInteger(i, 0)); break;
                    default: throw new RuntimeException("Unsupported data type: " + fieldTypes.get(i));
                }
            }
        }
        return data;
    }

}
