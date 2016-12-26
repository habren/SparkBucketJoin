package com.jasongj.spark.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public class Tuple {
    @Setter @Getter private List<Object> keys;
    @Setter @Getter private List<Object> data;

    public Tuple(List<Object> keys, List<Object> data) {
        this.keys = keys;
        this.data = data;
    }
}
