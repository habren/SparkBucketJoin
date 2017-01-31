package com.jasongj.spark.join;

import com.google.common.base.Preconditions;
import com.jasongj.spark.model.Tuple;
import org.apache.hadoop.hive.metastore.api.Decimal;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public class TupleComparator implements Comparator<Tuple> {

    @Override
    public int compare(Tuple tuple1, Tuple tuple2) {
        if(tuple1 == null) {
            if(tuple2 == null) {
                return 0;
            } else {
                return -1;
            }
        } else {
            if(tuple2 == null) {
                return 1;
            } else {
                List<Object> keys1 = tuple1.getKeys();
                List<Object> keys2 = tuple2.getKeys();
                for(int i = 0 ; i < keys1.size(); i++) {
                    Object key1 = keys1.get(i);
                    Object key2 = keys2.get(i);
                    int result = compareField(key1, key2);
                    if(result != 0) {
                        return result;
                    }
                }
                return 0;
            }
        }
    }

    private int compareField(Object field1, Object field2) {
        if(field1 == null) {
            if(field2 == null) {
                return 0;
            } else {
                return -1;
            }
        } else {
            if(field2 == null) {
                return 1;
            } else {
                Preconditions.checkArgument(field1.getClass().getCanonicalName().equals(field2.getClass().getCanonicalName()), "Key set for tuple1 and tuple2 should have the same type");
                String type = field1.getClass().getSimpleName().toLowerCase();
                String str1 = field1.toString();
                String str2 = field2.toString();
                switch(type) {
                    case "string": return String.valueOf(str1).compareTo(String.valueOf(str2));
                    case "bigdecimal": return BigDecimal.valueOf(Long.valueOf(str1)).compareTo(BigDecimal.valueOf(Long.valueOf(str2)));
                    case "integer": return Integer.valueOf(str1).compareTo(Integer.valueOf(str2));
                    case "long": return Long.valueOf(str1).compareTo(Long.valueOf(str2));
                    case "double": return Double.valueOf(str1).compareTo(Double.valueOf(str2));
                    case "float": return Float.valueOf(str1).compareTo(Float.valueOf(str2));
                    case "boolean": return Boolean.valueOf(str1).compareTo(Boolean.valueOf(str2));
                    case "date": return Date.valueOf(str1).compareTo(Date.valueOf(str2));
                    case "timestamp": return Timestamp.valueOf(str1).compareTo(Timestamp.valueOf(str2));
                    case "short":
                    case "smallint": return Short.valueOf(str1).compareTo(Short.valueOf(str2));
                    case "tinyint":
                    case "byte": return Byte.valueOf(str1).compareTo(Byte.valueOf(str2));
                    default: throw new RuntimeException("Unsupported data type: " + type);
                }
            }
        }
    }
    public static void main(String[] args) {
        Object obj = new Decimal();
        System.out.printf("name=%s, canonical name=%s, simplename=%s, typename=%s\n", obj.getClass().getName(), obj.getClass().getCanonicalName(), obj.getClass().getSimpleName(), obj.getClass().getTypeName());
    }
}
