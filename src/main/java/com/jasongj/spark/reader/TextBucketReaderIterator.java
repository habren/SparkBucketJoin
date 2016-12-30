package com.jasongj.spark.reader;

import com.google.common.base.Preconditions;
import com.jasongj.spark.model.Bucket;
import com.jasongj.spark.model.TableMetaData;
import com.jasongj.spark.model.Tuple;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
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
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public class TextBucketReaderIterator extends BucketReaderIterator {
    private static final Logger LOG = LoggerFactory.getLogger(TextBucketReaderIterator.class);

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private String fieldDelimiter;

    public TextBucketReaderIterator (Configuration hadoopConfiguration, TableMetaData tableMetaData, Integer bucketID) {
        super(hadoopConfiguration, tableMetaData, bucketID);
        TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(hadoopConfiguration, new TaskAttemptID());
        Bucket bucket = tableMetaData.getBuckets().get(bucketID);
        byte[] recordDelimiter = String.valueOf(tableMetaData.getLineDelimiter()).getBytes();
        this.fieldDelimiter = String.valueOf(String.valueOf(tableMetaData.getFieldDelimiter()));
        this.recordReader = new LineRecordReader(recordDelimiter);
        FileSplit fileSplit = new FileSplit(new Path(bucket.getUri()), 0, bucket.getLength(), null);
        try {
            recordReader.initialize(fileSplit, taskAttemptContext);
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
            String line = recordReader.getCurrentValue().toString();
            String[] record = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, fieldDelimiter);
            List<Object> data = convertDataWithType(record, this.fieldTypes);
            List<Object> keys = keyIndex.stream().map((Integer index) -> data.get(index)).collect(Collectors.toList());
            this.header = new Tuple(keys, data);
            return this.header;
        } catch (IOException | InterruptedException | ParseException ex) {
            LOG.warn("Fetch tuple failed", ex);
            return null;
        }
    }

    private List<Object> convertDataWithType(@NotNull String[] record, @NotNull List<String> types) throws ParseException {
        Preconditions.checkArgument(record.length == types.size(), "Record field number should be equal to type number");
        List<Object> data = new ArrayList<Object>();
        for(int i = 0; i < record.length; i++) {
            String dataStr = record[i].toString();
            if(StringUtils.isEmpty(dataStr)) {
                data.add(null);
            }
            switch (types.get(i).toLowerCase()) {
                case "string" : data.add(dataStr); break;
                case "bigdecimal": data.add(BigDecimal.valueOf(Long.valueOf(dataStr))); break;
                case "int": data.add(Integer.valueOf(dataStr)); break;
                case "long": data.add(Long.valueOf(dataStr)); break;
                case "double": data.add(Double.valueOf(dataStr)); break;
                case "float": data.add(Float.valueOf(dataStr)); break;
                case "boolean": data.add(Boolean.valueOf(dataStr)); break;
                case "date": data.add(new Date(dateFormat.parse(dataStr).getTime())); break;
                case "timestamp": data.add(new Timestamp(timestampFormat.parse(dataStr).getTime())); break;
                case "short":
                case "smallint": data.add(Short.valueOf(dataStr)); break;
                case "tinyint":
                case "byte": data.add(Byte.valueOf(dataStr)); break;
                default: throw new RuntimeException("Unsupported data type: " + types.get(i));
            }
        }
        return data;
    }

}
