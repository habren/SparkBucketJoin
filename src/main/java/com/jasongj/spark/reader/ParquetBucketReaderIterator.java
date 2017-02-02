package com.jasongj.spark.reader;

import com.google.common.base.Preconditions;
import com.jasongj.spark.model.Bucket;
import com.jasongj.spark.model.TableMetaData;
import com.jasongj.spark.model.Tuple;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */

public class ParquetBucketReaderIterator extends BucketReaderIterator {
    private static final Logger LOG = LoggerFactory.getLogger(ParquetBucketReaderIterator.class);
    private ParquetReader<GenericRecord> parquetReader;
    private List<String> fieldNames;

    public ParquetBucketReaderIterator(Configuration hadoopConfiguration, TableMetaData tableMetaData, Integer bucketID) {
        super(hadoopConfiguration, tableMetaData, bucketID);
        Bucket bucket = tableMetaData.getBuckets().get(bucketID);
        fieldNames = tableMetaData.getFields().stream().map((FieldSchema fieldSchema) -> fieldSchema.getName()).collect(Collectors.toList());Collectors.toList();
        try {
            /*ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(hadoopConfiguration, new Path(bucket.getUri()), ParquetMetadataConverter.NO_FILTER);
            MessageType parquetSchema = parquetMetadata.getFileMetaData().getSchema();
            Schema schema = new AvroSchemaConverter().convert(parquetSchema);*/
            parquetReader = AvroParquetReader.<GenericRecord>builder(new Path(bucket.getUri())).build();
        } catch (IOException ex) {
            LOG.error("Initial parquet reader failed", ex);
            throw new RuntimeException("Initial parquet reader failed", ex);
        }

    }

    public Tuple fetchTuple() {
        try {
            if(this.header != null) {
                return this.header;
            }

            GenericRecord record =  parquetReader.read();
            if(record == null) {
                return null;
            }
            List<Object> data = convertDataWithType(record, super.fieldTypes);
            List<Object> keys = keyIndex.stream().map((Integer index) -> data.get(index)).collect(Collectors.toList());
            this.header = new Tuple(keys, data);
            return this.header;
        } catch (IOException  ex) {
            LOG.warn("Fetch tuple failed", ex);
            throw new RuntimeException("Fetch tuple failed", ex);
        }
    }

    private List<Object> convertDataWithType(GenericRecord record, List<String> fieldTypes) {
        Preconditions.checkNotNull(record);
        Preconditions.checkNotNull(fieldTypes);
        List<Object> data = new ArrayList<Object>();
        return fieldNames.stream().map((String fieldName) -> {
            Object value = record.get(fieldName);
            if(value instanceof GenericData.Fixed) {
                GenericData.Fixed fixed = (GenericData.Fixed) value;
                value = ByteBuffer.wrap(fixed.bytes());
//                value = ByteBuffer.wrap(((GenericData.Fixed) value).bytes());
//                value = new HiveDecimalWritable().;
            }
            return value;
        }).collect(Collectors.toList());
    }

}
