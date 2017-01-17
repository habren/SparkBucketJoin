package com.jasongj.spark.writer;

import com.jasongj.spark.model.TableMetaData;
import com.jasongj.spark.model.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.TableMeta;

import java.io.DataOutputStream;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public abstract class TupleWriter {
    protected Path path;
    protected Configuration hadoopConfiguration;
    protected DataOutputStream dataOutputStream;
    protected TableMetaData outputTableMetaData;

    public TupleWriter(DataOutputStream dataOutputStream, Configuration hadoopConfiguration, Path path, TableMetaData outputTableMetaData) {
        this.dataOutputStream = dataOutputStream;
        this.hadoopConfiguration = hadoopConfiguration;
        this.path = path;
        this.outputTableMetaData = outputTableMetaData;
    }

    public boolean init() {
        return dataOutputStream != null && hadoopConfiguration != null && path != null && outputTableMetaData != null;
    }
    public abstract void write(Tuple tuple);
    public abstract void close();
}
