package com.jasongj.spark.model;

import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import javax.validation.constraints.NotNull;


/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public class TableMetaData implements Serializable{
    @Setter @Getter @NotNull private String database;
    @Setter @Getter @NotNull private String table;
    @Setter @Getter @NotNull private List<FieldSchema> fields;
    @Setter @Getter private List<Integer> bucketColumns;
    @Setter @Getter private List<Integer> sortColumns;
    @Setter @Getter @NotNull private URI location;
    @Setter @Getter @NotNull private int bucketNum;
    @Setter @Getter private char fieldDelimiter;
    @Setter @Getter private char lineDelimiter;
    @Setter @Getter private Map<Integer, Bucket> buckets = new HashMap<Integer, Bucket>();

    public TableMetaData() {}

    public TableMetaData(String database, String table, List<FieldSchema> fields, List<Integer> bucketColumns, List<Integer> sortColumns, URI location, int bucketNum) {
        this.database = database;
        this.table = table;
        this.fields = fields;
        this.bucketColumns = bucketColumns;
        this.sortColumns = sortColumns;
        this.location = location;
        this.bucketNum = bucketNum;
    }
}
