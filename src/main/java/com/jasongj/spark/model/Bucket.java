package com.jasongj.spark.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.fs.Path;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.net.URI;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public class Bucket implements Serializable{
    @Setter @Getter @NotNull private Integer index;
    @Setter @Getter @NotNull private URI uri;
    @Setter @Getter @NotNull private long length;

    public Bucket(URI uri, int index, long length) {
        this.uri = uri;
        this.index = index;
        this.length = length;
    }
}