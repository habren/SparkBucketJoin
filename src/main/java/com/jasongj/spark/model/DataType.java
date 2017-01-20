package com.jasongj.spark.model;

import com.jasongj.spark.reader.BucketReaderIterator;
import com.jasongj.spark.reader.ParquetBucketReaderIterator;
import com.jasongj.spark.reader.TextBucketReaderIterator;
import com.jasongj.spark.writer.ParquetTupleWriter;
import com.jasongj.spark.writer.TextTupleWriter;
import com.jasongj.spark.writer.TupleWriter;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public enum DataType {
    TEXT(TextBucketReaderIterator.class, TextTupleWriter.class, Arrays.asList("org.apache.hadoop.mapred.TextInputFormat",
            "org.apache.hadoop.mapreduce.lib.input.TextInputFormat", "org.apache.hadoop.mapred.TextOutputFormat",
            "org.apache.hadoop.mapreduce.lib.output.TextOutputFormat")),
    PARQUET(ParquetBucketReaderIterator.class, ParquetTupleWriter.class, Arrays.asList("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "org.apache.parquet.hadoop.ParquetInputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "org.apache.parquet.hadoop.ParquetOutputFormat"));

    @Getter
    private Class<? extends BucketReaderIterator> readerClass;
    @Getter
    private Class<? extends TupleWriter> writerClass;
    @Getter private List<String> inputFormats;

    DataType(Class<? extends BucketReaderIterator> readerClass, Class<? extends TupleWriter> writerClass, List<String> inputFormats) {
        this.readerClass = readerClass;
        this.writerClass = writerClass;
        this.inputFormats = inputFormats;
    }
}