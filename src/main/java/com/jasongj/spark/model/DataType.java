package com.jasongj.spark.model;

import com.jasongj.spark.reader.BucketReaderIterator;
import com.jasongj.spark.reader.ParquetBucketReaderIterator;
import com.jasongj.spark.reader.TextBucketReaderIterator;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public enum DataType {
    TEXT(TextBucketReaderIterator.class, Arrays.asList("org.apache.hadoop.mapred.TextInputFormat", "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")),
    PARQUET(ParquetBucketReaderIterator.class, Arrays.asList("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", "org.apache.parquet.hadoop.ParquetInputFormat"));

    @Getter
    private Class<? extends BucketReaderIterator> readClass;
    @Getter private List<String> inputFormats;

    DataType(Class<? extends BucketReaderIterator> readClass, List<String> inputFormats) {
        this.readClass = readClass;
        this.inputFormats = inputFormats;
    }
}