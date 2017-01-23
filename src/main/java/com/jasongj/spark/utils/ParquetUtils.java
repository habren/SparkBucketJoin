package com.jasongj.spark.utils;

import com.jasongj.spark.model.DataType;
import com.jasongj.spark.model.TableMetaData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by juguo on 1/22/17.
 */
public class ParquetUtils {
    private static Pattern DECIMAL_PATTERN = Pattern.compile("decimal\\((\\d+),\\s*(\\d+)\\)");

    public static Optional<String> extractAvroSchemaFromParquet (TableMetaData tableMetaData) {
        if(tableMetaData.getDataType() != DataType.PARQUET) {
            return Optional.ofNullable(null);
        }
        List<String> fieldsList = tableMetaData.getFields().stream()
                .collect(Collectors.toMap(FieldSchema::getName, (FieldSchema fieldSchema) -> {
                    String type = fieldSchema.getType().toLowerCase();
                    String avroType = null;
                    switch (type) {
                        case "tinyint": avroType = "int"; break;
                        case "smallint": avroType = "int"; break;
                        case "int": avroType = "int"; break;
                        case "integer": avroType = "int"; break;
                        case "boolean": avroType = "boolean"; break;
                        case "long": avroType = "long"; break;
                        case "bigint": avroType = "long"; break;
                        case "float": avroType = "float"; break;
                        case "double": avroType = "double"; break;
                        case "string": avroType = "string"; break;
                        case "varchar": avroType = "string"; break;
                        case "char": avroType = "string"; break;
                        case "void": avroType = "null"; break;
                        case "binary": avroType = "bytes"; break;
                        case "struct": avroType = "record"; break;
                        case "date": {
                            avroType = "{\"type\": \"int\", \"logicalType\": \"date\"}";
                            break;
                        }
                        case "timestamp": avroType = "bytes"; break;
                        default: {
                            if(type.startsWith("decimal")) {
                                Matcher matcher = DECIMAL_PATTERN.matcher(type);
                                if(matcher.matches() && matcher.groupCount() == 2) {
                                    int precision = Integer.parseInt(matcher.group(1));
                                    int scale = Integer.parseInt(matcher.group(2));
                                    avroType = String.format("{\"type\": \"bytes\", \"logicalType\":\"decimal\", \"precision\": %d, \"scale\": %d}", precision, scale);
                                } else {
                                    throw new IllegalArgumentException("type(" + type +") parse failed");
                                }

                            } else {
                                avroType = "string";
                            }
                        }
                    }
                    return avroType;
                }))
                .entrySet()
                .stream()
                .map((Map.Entry<String, String> entry) -> String.format("{\"name\":\"%s\",\"type\":[%s, \"null\"]}", entry.getKey(), entry.getValue().startsWith("{") ? entry.getValue() : "\"" + entry.getValue() + "\""))
                .collect(Collectors.toList());
        String schemaStr = String.format("{\"type\":\"record\",\"name\":\"%s\", \"fields\":[%s]}", tableMetaData.getTable(), StringUtils.join(",", fieldsList));
        return Optional.ofNullable(schemaStr);
    }
}
