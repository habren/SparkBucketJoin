package com.jasongj.spark.writer;

import com.jasongj.spark.model.TableMetaData;
import com.jasongj.spark.model.Tuple;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.DataOutputStream;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public abstract class TupleWriter {
    protected Path path;
    protected Configuration hadoopConfiguration;
    protected TableMetaData outputTableMetaData;

    public TupleWriter(Configuration hadoopConfiguration, Path path, TableMetaData outputTableMetaData) {
        this.hadoopConfiguration = hadoopConfiguration;
        this.path = path;
        this.outputTableMetaData = outputTableMetaData;
    }

    public boolean init() {
        return hadoopConfiguration != null && path != null && outputTableMetaData != null;
    }
    public abstract void write(Tuple tuple);
    public abstract void close();
}
