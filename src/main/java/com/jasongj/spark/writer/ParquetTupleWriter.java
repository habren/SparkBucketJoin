package com.jasongj.spark.writer;

import com.jasongj.spark.model.DataType;
import com.jasongj.spark.model.TableMetaData;
import com.jasongj.spark.model.Tuple;
import com.jasongj.spark.utils.ParquetUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.parquet.Preconditions;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import parquet.org.slf4j.Logger;
import parquet.org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public class ParquetTupleWriter extends TupleWriter{
    private static final Logger LOG = LoggerFactory.getLogger(ParquetTupleWriter.class);
    private static final int PAGE_SIZE = 1024 * 1024;
    private static final int ROW_GROUP_SIZE = 1024 * 1024 * 1024;
    private Schema schema;
    ParquetWriter<GenericRecord> parquetWriter;

    public ParquetTupleWriter(Configuration hadoopConfiguration, Path path, TableMetaData outputTableMetaData) throws IOException {
        super(hadoopConfiguration, path, outputTableMetaData);
    }

    @Override
    public boolean init() {
        if(!super.init() && outputTableMetaData.getDataType() != DataType.PARQUET) {
            return false;
        }
        Optional<String> avroSchema = ParquetUtils.extractAvroSchemaFromParquet(outputTableMetaData);
        if(!avroSchema.isPresent()) {
            LOG.error("Extract Avro schema from parquet table {}.{} failed", outputTableMetaData.getDatabase(), outputTableMetaData.getTable());
            return false;
        }
        String schemaStr = avroSchema.get();
        LOG.info("Avro schmea for AvroParquetWriter: {}", schemaStr);
        try {
            this.schema = new Schema.Parser().parse(schemaStr);
            /*parquetWriter = AvroParquetWriter
                    .<GenericRecord>builder(path)
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .withPageSize(PAGE_SIZE)
                    .withRowGroupSize(ROW_GROUP_SIZE)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build();*/
            parquetWriter = new AvroParquetWriter<GenericRecord>(path, schema, CompressionCodecName.UNCOMPRESSED, ROW_GROUP_SIZE, PAGE_SIZE, false, hadoopConfiguration);
            return true;
        } catch (Exception ex) {
            LOG.warn("Init ParquetTupleWriterFailed", ex);
            return false;
        }
    }

    @Override
    public void write(Tuple tuple) {
        List<FieldSchema> fieldSchemas = outputTableMetaData.getFields();
        List<Object> data = tuple.getData();
        Preconditions.checkNotNull(data, "Tuple data should not be null");
        Preconditions.checkState(fieldSchemas.size() == data.size(),
                "keys number (%s) should equal to data field number (%s)", fieldSchemas.size(), data.size());
        GenericRecord record = new GenericData.Record(schema);
        for(int i = 0; i < fieldSchemas.size(); i++) {
            String name = fieldSchemas.get(i).getName();
            Object value =data.get(i);
            /*switch (fieldSchemas.get(i).getType()) {
                case "string": record.put(name, value); break;
                case "int": record.put(name, Integer.valueOf(String.valueOf(value))); break;
            }*/
            record.put(name, value);

        }
        try {
            parquetWriter.write(record);
        } catch (IOException ex) {
            throw new RuntimeException("Write Tuple with ParquetTupleWriter exception", ex);
        }
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(parquetWriter);
    }

}
