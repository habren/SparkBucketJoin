package com.jasongj.spark.writer;

import com.jasongj.spark.model.DataType;
import com.jasongj.spark.model.TableMetaData;
import com.jasongj.spark.model.Tuple;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.TableMeta;

import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public class TextTupleWriter extends TupleWriter {

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private String fieldDelimiter, lineDelimiter;

    public TextTupleWriter(DataOutputStream dataOutputStream, Configuration hadoopConfiguration, Path path, TableMetaData outputTableMetaData) {
        super(dataOutputStream, hadoopConfiguration, path, outputTableMetaData);
        this.fieldDelimiter = String.valueOf(outputTableMetaData.getFieldDelimiter());
        this.lineDelimiter = String.valueOf(outputTableMetaData.getLineDelimiter());
    }

    @Override
    public boolean init() {
        return super.init() && outputTableMetaData.getDataType() == DataType.TEXT;
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
        if(dataOutputStream != null) {
            IOUtils.closeQuietly(dataOutputStream);
            dataOutputStream = null;
        }
    }
}
