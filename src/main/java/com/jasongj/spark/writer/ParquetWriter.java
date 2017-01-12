package com.jasongj.spark.writer;

import com.jasongj.spark.model.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.DataOutputStream;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public class ParquetWriter extends TupleWriter{

    public ParquetWriter(DataOutputStream dataOutputStream, Configuration hadoopConfiguration, Path path) {
        super(dataOutputStream, hadoopConfiguration, path);
    }

    @Override
    public void write(Tuple tuple) {

    }

    @Override
    public void close() {

    }

    public static void main(String[] args) {

    }
}
