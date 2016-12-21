package com.jasongj.spark;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

public class JoinMetaData {
    @Setter @Getter private String table;
    @Setter @Getter private List<String> keys;
    @Setter @Getter private int bucketNum;

    public JoinMetaData(String table, List<String> keys, int bucketNum) {
        this.table = table;
        this.keys = keys;
        this.bucketNum = bucketNum;
    }
}
