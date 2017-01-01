package com.jasongj.spark.utils;

import com.google.common.base.Preconditions;
import com.jasongj.spark.model.DataType;
import com.jasongj.spark.model.TableMetaData;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public class HiveMetaDataExtractor {
    private static final Logger LOG = LoggerFactory.getLogger(HiveMetaDataExtractor.class);
    private static final String FIELD_DELIMITER_NAME = "field.delim";
    private static final String LINE_DELIMITER_NAME = "line.delim";

    private HiveConf hiveConf;
    private HiveMetaStoreClient hiveMetaStoreClient;

    public HiveMetaDataExtractor(HiveConf hiveConf) {
        this.hiveConf = hiveConf;
        try {
            this.hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    public TableMetaData getMetaData(String databaseName, String tableName) throws TException {
        Preconditions.checkNotNull(databaseName, "Database name should not be null");
        Preconditions.checkNotNull(tableName, "Table name should not be null");

        Table table = hiveMetaStoreClient.getTable(databaseName, tableName);
        StorageDescriptor storageDescriptor = table.getSd();
        Preconditions.checkArgument(storageDescriptor.getBucketColsSize() > 0, "Bucket column set for {}.{} should not be empty", databaseName, tableName);
        Preconditions.checkArgument(storageDescriptor.getSortColsSize() > 0, "Sorted column set for {}.{} should not be empty", databaseName, tableName);

        AtomicInteger index = new AtomicInteger(0);
        List<FieldSchema> fieldSchemas = hiveMetaStoreClient.getSchema(databaseName, tableName);
        Map<String, Integer> fieldNameIndexMap = fieldSchemas.stream().collect(Collectors.toMap(FieldSchema::getName, (fieldSchema) -> Integer.valueOf(index.getAndIncrement())));

        List<Integer> bucketColumns = storageDescriptor.getBucketCols().stream().map((String column) -> fieldNameIndexMap.get(column)).collect(Collectors.toList());
        List<Integer> sortColumns = storageDescriptor.getSortCols().stream().map((Order order) -> fieldNameIndexMap.get(order.getCol())).collect(Collectors.toList());

        String location = storageDescriptor.getLocation();
        int bucketNum = storageDescriptor.getNumBuckets();

        DataType dataFormatType = null;
        String type = storageDescriptor.getInputFormat();
        for(DataType dataType : DataType.values()) {
            if(dataType.getInputFormats().contains(type)) {
                dataFormatType = dataType;
                break;
            }
        }
        Preconditions.checkNotNull(dataFormatType, "Unsupported input data format : %s", type);
        TableMetaData tableMetaData = new TableMetaData(databaseName, tableName, fieldSchemas, bucketColumns, sortColumns, new Path(location).toUri(), bucketNum, dataFormatType);

        Map<String, String> parameters = storageDescriptor.getSerdeInfo().getParameters();
        if(parameters.containsKey(FIELD_DELIMITER_NAME)) {
            tableMetaData.setFieldDelimiter(parameters.get(FIELD_DELIMITER_NAME).toCharArray()[0]);
        }
        if(parameters.containsKey(LINE_DELIMITER_NAME)) {
            tableMetaData.setLineDelimiter(parameters.get(LINE_DELIMITER_NAME).toCharArray()[0]);
        }
        return tableMetaData;
    }

    public void close() {
        if(hiveMetaStoreClient != null) {
            hiveMetaStoreClient.close();
        }
    }
}
