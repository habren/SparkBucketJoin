package com.jasongj.spark.writer;

import com.jasongj.spark.model.DataType;
import com.jasongj.spark.model.TableMetaData;
import com.jasongj.spark.model.Tuple;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.util.StringUtils;
import org.apache.parquet.Preconditions;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import parquet.org.slf4j.Logger;
import parquet.org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public class ParquetTupleWriter extends TupleWriter{
    private static final Logger LOG = LoggerFactory.getLogger(ParquetTupleWriter.class);
    private static final int PAGE_SIZE = 1024 * 1024;
    private static final int ROW_GROUP_SIZE = 1024 * 1024 * 1024;
    private Schema schema;
    ParquetWriter<GenericRecord> parquetWriter;

    public ParquetTupleWriter(DataOutputStream dataOutputStream, Configuration hadoopConfiguration, Path path, TableMetaData outputTableMetaData) throws IOException {
        super(dataOutputStream, hadoopConfiguration, path, outputTableMetaData);
    }

    @Override
    public boolean init() {
        if(!super.init() && outputTableMetaData.getDataType() != DataType.PARQUET) {
            return false;
        }
        List<String> fieldsList = outputTableMetaData.getFields().stream()
                .collect(Collectors.toMap(FieldSchema::getName, (FieldSchema fieldSchema) -> {
                    String type = fieldSchema.getType().toLowerCase();
                    String avroType = null;
                    switch (type) {
                        case "integer": avroType = "int"; break;
                        case "boolean": avroType = "boolean"; break;
                        case "long": avroType = "long"; break;
                        case "float": avroType = "float"; break;
                        case "double": avroType = "double"; break;
                        case "string": avroType = "string"; break;
                        default: avroType = "string";
                    }
                    return avroType;
                }))
                .entrySet()
                .stream()
                .map((Map.Entry<String, String> entry) -> String.format("\"name\":\"%s\",\"type\":\"%s\"", entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        String schemaStr = String.format("{\"type\":\"record\",\"name\":\"%s\", \"fields\":[%s]}", outputTableMetaData.getTable(), StringUtils.join(",", fieldsList));
        LOG.info("Avro schmea for AvroParquetWriter: {}", schemaStr);
        this.schema = new Schema.Parser().parse(schemaStr);
        try {
            parquetWriter = AvroParquetWriter
                    .<GenericRecord>builder(path)
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .withPageSize(PAGE_SIZE)
                    .withRowGroupSize(ROW_GROUP_SIZE)
                    .build();
            return true;
        } catch (IOException ex) {
            LOG.warn("Init ParquetTupleWriterFailed", ex);
            return false;
        }
    }

    @Override
    public void write(Tuple tuple) {
        List<FieldSchema> fieldSchemas = outputTableMetaData.getFields();
        List<Object> data = tuple.getData();
        Preconditions.checkState(data == null || fieldSchemas.size() != data.size(),
                "keys number (%d) should equal to data field number (%d)", fieldSchemas.size(), data.size());
        GenericRecord record = new GenericData.Record(schema);
        for(int i = 0; i < fieldSchemas.size(); i++) {
            record.put(fieldSchemas.get(i).getName(), data.get(i));
        }
        try {
            parquetWriter.write(record);
        } catch (IOException ex) {
            throw new RuntimeException("Write Tuple with ParquetTupleWriter exception", ex);
        }
    }

    @Override
    public void close() {
        if(dataOutputStream != null) {
            try {
                dataOutputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            IOUtils.closeQuietly(dataOutputStream);
        }

    }

}
