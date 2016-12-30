package com.jasongj.spark.writer;

import com.jasongj.spark.model.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;

import java.io.DataOutputStream;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public abstract class TupleWriter {
    protected Path path;
    protected Configuration hadoopConfiguration;
    protected RecordWriter recordWriter;
    protected DataOutputStream dataOutputStream;

    public TupleWriter(DataOutputStream dataOutputStream, Configuration hadoopConfiguration, Path path) {
        this.dataOutputStream = dataOutputStream;
        this.hadoopConfiguration = hadoopConfiguration;
        this.path = path;
    }

    public abstract void write(Tuple tuple);
    public abstract void close();
}
