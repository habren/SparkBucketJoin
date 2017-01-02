package com.jasongj.spark.model;

import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.net.URI;
import java.util.concurrent.CountDownLatch;

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

    public static void main(String[] args) throws InterruptedException {
        int threads = 1000;
        long start = System.currentTimeMillis();
        CountDownLatch countDownLatch = new CountDownLatch(threads);
        for(int i = 0; i < threads; i++) {
            new Thread(()-> {
                try{
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {

                }
                countDownLatch.countDown();
            }).start();
        }
        countDownLatch.await();
        long stop = System.currentTimeMillis();
        System.out.println("Finished " + (stop - start));
    }
}