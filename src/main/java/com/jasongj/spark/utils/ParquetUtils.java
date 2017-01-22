package com.jasongj.spark.utils;

import com.jasongj.spark.model.DataType;
import com.jasongj.spark.model.TableMetaData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by juguo on 1/22/17.
 */
public class ParquetUtils {
    public static Optional<String> extractAvroSchemaFromParquet (TableMetaData tableMetaData) {
        if(tableMetaData.getDataType() != DataType.PARQUET) {
            return Optional.ofNullable(null);
        }
        List<String> fieldsList = tableMetaData.getFields().stream()
                .collect(Collectors.toMap(FieldSchema::getName, (FieldSchema fieldSchema) -> {
                    String type = fieldSchema.getType().toLowerCase();
                    String avroType = null;
                    switch (type) {
                        case "integer": avroType = "int"; break;
                        case "int32": avroType = "int"; break;
                        case "int": avroType = "int"; break;
                        case "boolean": avroType = "boolean"; break;
                        case "long": avroType = "long"; break;
                        case "int64": avroType = "long"; break;
                        case "float": avroType = "float"; break;
                        case "double": avroType = "double"; break;
                        case "string": avroType = "string"; break;
                        default: avroType = "string";
                    }
                    return avroType;
                }))
                .entrySet()
                .stream()
                .map((Map.Entry<String, String> entry) -> String.format("{\"name\":\"%s\",\"type\":\"%s\"}", entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        String schemaStr = String.format("{\"type\":\"record\",\"name\":\"%s\", \"fields\":[%s]}", tableMetaData.getTable(), StringUtils.join(",", fieldsList));
        return Optional.ofNullable(schemaStr);
    }
}
