package com.jasongj.spark.writer;

import com.jasongj.spark.model.DataType;
import com.jasongj.spark.model.TableMetaData;
import com.jasongj.spark.model.Tuple;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.org.slf4j.Logger;
import parquet.org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public class TextTupleWriter extends TupleWriter {
    private static final Logger LOG = LoggerFactory.getLogger(TextTupleWriter.class);

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private String fieldDelimiter, lineDelimiter;
    private FSDataOutputStream dataOutputStream;

    public TextTupleWriter(Configuration hadoopConfiguration, Path path, TableMetaData outputTableMetaData) {
        super(hadoopConfiguration, path, outputTableMetaData);
        this.fieldDelimiter = String.valueOf(outputTableMetaData.getFieldDelimiter());
        this.lineDelimiter = String.valueOf(outputTableMetaData.getLineDelimiter());
    }

    @Override
    public boolean init() {
        if(super.init() && outputTableMetaData.getDataType() == DataType.TEXT) {
            FileSystem fileSystem = null;
            try {
                fileSystem = FileSystem.newInstance(hadoopConfiguration);
                dataOutputStream = fileSystem.create(path, true);
                return true;
            } catch (IOException ex) {
                LOG.error("Init TextTupleWriter failed", ex);
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public void write(Tuple tuple) {
        String data = StringUtils.join(tuple.getData().stream().map((Object object) -> {
            if(object == null) {
                return StringUtils.EMPTY;
            } else if(object instanceof Date) {
                return dateFormat.format((Date) object);
            } else if(object instanceof Timestamp) {
                return timestampFormat.format((Timestamp) object);
            } else {
                return object.toString();
            }
        }).toArray(), fieldDelimiter) + lineDelimiter;
        try {
            dataOutputStream.write(data.getBytes());
        } catch (IOException ex) {
            throw new RuntimeException("Write Tuple with TextTupleWriter exception", ex);
        }
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(dataOutputStream);
    }

}
