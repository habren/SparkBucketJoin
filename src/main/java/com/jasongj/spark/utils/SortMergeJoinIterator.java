package com.jasongj.spark.utils;

import com.google.common.base.Preconditions;
import com.jasongj.spark.model.Tuple;
import com.jasongj.spark.reader.BucketReaderIterator;

import javax.validation.constraints.NotNull;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public class SortMergeJoinIterator implements Iterator<Tuple>{

    @NotNull private static final Comparator<Tuple> comparator = new TupleComparator();
    @NotNull private BucketReaderIterator baseIterator;
    @NotNull private BucketReaderIterator deltaIterator;
    private Tuple baseHeader;
    private Tuple deltaHeader;


    public SortMergeJoinIterator(BucketReaderIterator baseIterator, BucketReaderIterator deltaIterator) {
        Preconditions.checkNotNull(baseIterator);
        Preconditions.checkNotNull(deltaIterator);
        this.baseIterator = baseIterator;
        this.deltaIterator = deltaIterator;
    }

    @Override
    public boolean hasNext() {
        if(baseIterator.hasNext() || deltaIterator.hasNext()) {
            return true;
        }
        return false;
    }

    @Override
    public Tuple next() {
        baseHeader = baseIterator.preFetch();
        deltaHeader = deltaIterator.preFetch();
        Tuple tuple = null;
        if(baseHeader == null) {
            tuple = deltaIterator.next();
        } else if(deltaHeader == null){
            tuple = baseIterator.next();
        } else {
            int flag = comparator.compare(baseHeader, deltaHeader);
            if(flag >= 0) {
                tuple = deltaIterator.next();
                if(flag == 0) {
                    baseIterator.next();
                }
            } else {
                tuple = baseIterator.next();
            }
        }
        return tuple;
    }
}
