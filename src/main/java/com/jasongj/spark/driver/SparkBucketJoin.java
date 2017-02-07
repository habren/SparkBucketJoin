package com.jasongj.spark.driver;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.jasongj.spark.model.Tuple;
import com.jasongj.spark.reader.BucketReaderIterator;
import com.jasongj.spark.utils.HiveMetaDataExtractor;
import com.jasongj.spark.model.TableMetaData;
import com.jasongj.spark.model.Bucket;
import com.jasongj.spark.join.SortMergeJoinIterator;
import com.jasongj.spark.writer.TupleWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.parquet.Preconditions;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public class SparkBucketJoin {

    private static final Logger LOG = LoggerFactory.getLogger(SparkBucketJoin.class);
    private static final String DEFAULT_APP_NAME = "SparkBucketJoin";

    private static String baseDB;
    private static String baseTable;
    private static String deltaDB;
    private static String deltaTable;
    private static String outputDB;
    private static String outputTable;
    private static List<String> joinKeys;
    private static boolean localMode;
    private static String sparkAppName;
    private static HiveConf hiveConf;
    private static Configuration hadoopConfiguration;

    public static void main(String[] args) throws TException, InterruptedException {
        if(!parseArguments(args)) {
            return;
        }

        HiveMetaDataExtractor hiveMetaDataExtractor = new HiveMetaDataExtractor(hiveConf);
        TableMetaData baseTableMetaData = hiveMetaDataExtractor.getMetaData(baseDB, baseTable);
        TableMetaData deltaTableMetaData = hiveMetaDataExtractor.getMetaData(deltaDB, deltaTable);
        TableMetaData outputTableMetaData = hiveMetaDataExtractor.getMetaData(outputDB, outputTable);

        Preconditions.checkArgument(baseTableMetaData.getBucketNum() == deltaTableMetaData.getBucketNum()
                && baseTableMetaData.getBucketNum() == outputTableMetaData.getBucketNum()
                , "Base table, delta table and output table should have the same bucket number"
        );

        if (!checkAndLoadFile(hadoopConfiguration, baseTableMetaData)
                || !checkAndLoadFile(hadoopConfiguration, deltaTableMetaData)
                || !makeOutputTableReady(hadoopConfiguration, outputTableMetaData)) {
            return;
        }

        SparkConf sparkConf = new SparkConf().setAppName(sparkAppName);
        if(localMode) {
            sparkConf.setMaster("local[3]");
        }
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.hadoopConfiguration().addResource(hadoopConfiguration);


        Broadcast<TableMetaData> broadcastBaseTable = javaSparkContext.broadcast(baseTableMetaData);
        Broadcast<TableMetaData> broadcastDeltaTable = javaSparkContext.broadcast(deltaTableMetaData);
        Broadcast<TableMetaData> broadcastOutputTable = javaSparkContext.broadcast(outputTableMetaData);

        Map<String, String> hadoopConf = new ConcurrentHashMap<String, String>();
        hadoopConfiguration.forEach((Map.Entry<String, String> entry) -> hadoopConf.put(entry.getKey(), entry.getValue()));
        Broadcast<Map<String, String>> configurationBroadcast = javaSparkContext.broadcast(hadoopConf);

        int buckets = baseTableMetaData.getBucketNum();
        List<Integer> list = new ArrayList<Integer>();
        for(int i = 0; i < buckets; i++) {
            list.add(i);
        }
        JavaRDD<Tuple> rdd = javaSparkContext.parallelize(list, baseTableMetaData.getBucketNum())
                .mapPartitionsWithIndex((Integer index, Iterator<Integer> integerIterator) -> {
                    TableMetaData baseTable = broadcastBaseTable.value();
                    TableMetaData deltaTable = broadcastDeltaTable.value();
                    Map<String, String> hadoopConfigurationMap = configurationBroadcast.value();
                    Configuration configuration = new Configuration();
                    hadoopConfigurationMap.forEach((String key, String value) -> configuration.set(key, value));

                    Class<? extends BucketReaderIterator> baseReaderClass = baseTable.getDataType().getReaderClass();
                    BucketReaderIterator baseIterator = baseReaderClass.getConstructor(Configuration.class, TableMetaData.class, Integer.class).newInstance(configuration, baseTable, index);
                    Class<? extends BucketReaderIterator> deltaReaderClass = baseTable.getDataType().getReaderClass();
                    BucketReaderIterator deltaIterator = deltaReaderClass.getConstructor(Configuration.class, TableMetaData.class, Integer.class).newInstance(configuration, deltaTable, index);
                    SortMergeJoinIterator sortMergeJoinIterator = new SortMergeJoinIterator(baseIterator, deltaIterator);
                    return sortMergeJoinIterator;
                }, false);

        rdd.foreachPartition((Iterator<Tuple> iterator) -> {
            Logger LOG = LoggerFactory.getLogger("TupleWriter");
            TaskContext taskContext = TaskContext.get();
            int partitionIndex = taskContext.partitionId();
            int attemptNumber = taskContext.attemptNumber();
            Path path = new Path(outputTableMetaData.getLocation().toString() + getTmpPathName(attemptNumber, partitionIndex));
            Map<String, String> hadoopConfigurationMap = configurationBroadcast.value();
            Configuration configuration = new Configuration();
            hadoopConfigurationMap.forEach((String key, String value) -> configuration.set(key, value));
            FileSystem fileSystem = FileSystem.newInstance(configuration);

            TableMetaData outputTable = broadcastOutputTable.value();
            Class<? extends TupleWriter> tupleWriterClass = outputTable.getDataType().getWriterClass();
            TupleWriter tupleWriter = tupleWriterClass.getConstructor(Configuration.class, Path.class, TableMetaData.class).newInstance(configuration, path, outputTable);
            tupleWriter.init();
            iterator.forEachRemaining((Tuple tuple) -> tupleWriter.write(tuple));
            tupleWriter.close();
            Path finalPath = new Path(outputTableMetaData.getLocation().toString() + getBucketPathName(partitionIndex));
            fileSystem.rename(path, finalPath);
            taskContext.addTaskCompletionListener((TaskContext context) -> {
//                try {
//                    fileSystem.rename(path, finalPath);
//                } catch (IOException ex) {
//                    LOG.warn(String.format("Failed to rename %s to %s", path, finalPath), ex);
//                }
            });
            taskContext.addTaskFailureListener((TaskContext context, Throwable throwable) -> {
                LOG.warn("Task failed ", throwable);
                try {
                    fileSystem.delete(path, true);
                } catch (IOException ex) {
                    LOG.warn("Failed to delete temporary file " + path.getName(), ex);
                }

            });
        });


        hiveMetaDataExtractor.close();
        javaSparkContext.stop();
    }

    private static String getTmpPathName(int attemptNumber, int index) {
        return String.format("%s.%d.tmp", getBucketPathName(index), attemptNumber);
    }

    private static String getBucketPathName(int index) {
        return "/" + StringUtils.leftPad(String.valueOf(index), 6, "0") + "_0";
    }

    private static boolean checkAndLoadFile(Configuration hadoopConfiguration, TableMetaData tableMetaData) {
        try {
            Path path = new Path(tableMetaData.getLocation());
            FileSystem fileSystem = FileSystem.newInstance(hadoopConfiguration);
            if(!fileSystem.exists(path)) {
                LOG.error("Path '{}' does not exist", tableMetaData.getLocation());
                return false;
            }
            if(!fileSystem.isDirectory(path)) {
                LOG.error("Path '{}' is not a valid directory", tableMetaData.getLocation());
                return false;
            }
            Map<Integer, Bucket> buckets = Stream.of(fileSystem.listStatus(path)).map((FileStatus fileStatus) -> {
                Path filePath = fileStatus.getPath();
                String baseName = FilenameUtils.getBaseName(filePath.getName());

                // Extract bucket sequence from TAZ based hive table bucket file name
                Pattern bucketFileTAZPattern = Pattern.compile("(\\d+)_(\\d+)");
                Matcher matcherTAZ = bucketFileTAZPattern.matcher(baseName);
                if(matcherTAZ.matches()) {
                    int index = Integer.parseInt(matcherTAZ.group(1));
                    return new Bucket(filePath.toUri(), index, fileStatus.getLen());
                }

                // Extract bucket sequence from Map Reduce based hive table bucket file name
                Pattern bucketFileMRPattern = Pattern.compile("part-(\\d+)");
                Matcher matcherMR = bucketFileMRPattern.matcher(baseName);
                if(matcherMR.matches()) {
                    int index = Integer.parseInt(matcherMR.group(1));
                    return new Bucket(filePath.toUri(), index, fileStatus.getLen());
                }
                return null;
            }).filter((Bucket bucket) -> bucket != null)
            .collect(Collectors.toMap(Bucket::getIndex, Function.identity()));
            tableMetaData.setBuckets(buckets);
            return true;
        } catch (IOException ex) {
            LOG.error("Check file failed", ex);
            return false;
        }
    }

    private static boolean makeOutputTableReady(Configuration hadoopConfiguration, TableMetaData tableMetaData) {
        try{
            Path path = new Path(tableMetaData.getLocation());
            FileSystem fileSystem = FileSystem.newInstance(hadoopConfiguration);
            if(fileSystem.exists(path)) {
                for(FileStatus fileStatus : fileSystem.listStatus(path)) {
                    try {
                        fileSystem.delete(fileStatus.getPath(), true);
                        LOG.info("Delete file {} successfully", fileStatus.getPath().getName());
                    } catch (IOException ex) {
                        LOG.error("Delete file " + fileStatus.getPath().getName() + " failed", ex);
                        return false;
                    }
                }
            } else {
                if(fileSystem.mkdirs(path)) {
                    LOG.info("Successfully create the target folder {}", path.getName());
                } else {
                    LOG.error("Failed to create the target folder {}", path.getName());
                    return false;
                }
            }
            return true;
        } catch (IOException ex) {
            LOG.error("Didn't make the output table directory ready");
            return false;
        }
    }

    private static boolean parseArguments(String[] args) {
        Options options = new Options();
        HelpFormatter formatter = new HelpFormatter();
        try{
            /*
            options.addOption(Option.builder("b").required(true).argName("database.table").longOpt("base_table").valueSeparator('.').hasArgs().desc("Base table name, including database name").build());
            options.addOption(Option.builder("d").required(true).argName("database.table").longOpt("delta_table").valueSeparator('.').hasArgs().desc("Incremental table name, including database name").build());
            options.addOption(Option.builder("o").required(true).argName("database.table").longOpt("output_table").valueSeparator('.').hasArgs().desc("Output table name, including database name").build());
            options.addOption(Option.builder("k").required(true).argName("join key").longOpt("join_key").hasArg(true).desc("Join key").build());
            options.addOption(Option.builder("m").required(false).argName("spark mode").longOpt("spark_mode").hasArg(true).desc("Spark execution mode").build());
            options.addOption(Option.builder("s").required(false).argName("hive-site.xml").longOpt("hive-site").hasArg(true).desc("Hive configuration file. eg. hive-site.xml").build());
            options.addOption(Option.builder("a").required(false).argName("core-site.xml").longOpt("core-site").hasArg(true).desc("Hadoop configuration file. eg. core-site.xml").build());
            options.addOption(Option.builder("n").required(false).argName("spark app name").longOpt("spark_app_name").hasArg(true).desc("Spark Application Name").build());
            options.addOption(Option.builder("p").required(true).argName("key=value").longOpt("properties").valueSeparator('=').hasArgs().desc("Hive properties").build());
            options.addOption(Option.builder("h").required(false).longOpt("help").hasArg(false).desc("Help").build());
            CommandLineParser parser = new DefaultParser();
            */

            options.addOption(OptionBuilder.isRequired(true).withArgName("database.table").withLongOpt("base-table").withValueSeparator('.').hasArgs().withDescription("Base table name, including database name").create("b"));
            options.addOption(OptionBuilder.isRequired(true).withArgName("database.table").withLongOpt("delta-table").withValueSeparator('.').hasArgs().withDescription("Incremental table name, including database name").create("d"));
            options.addOption(OptionBuilder.isRequired(true).withArgName("database.table").withLongOpt("output-table").withValueSeparator('.').hasArgs().withDescription("Output table name, including database name").create("o"));
            options.addOption(OptionBuilder.isRequired(true).withArgName("join key").withLongOpt("join-key").hasArg(true).withDescription("Join key").create("k"));
            options.addOption(OptionBuilder.isRequired(false).withArgName("local mode").withLongOpt("local-mode").hasArg(false).withDescription("Local mode").create("l"));
            options.addOption(OptionBuilder.isRequired(false).withArgName("hive-site.xml").withLongOpt("hive-site").hasArg(true).withDescription("Hive configuration file. eg. hive-site.xml").create("s"));
            options.addOption(OptionBuilder.isRequired(false).withArgName("core-site.xml").withLongOpt("core-site").hasArg(true).withDescription("Hadoop configuration file. eg. core-site.xml").create("a"));
            options.addOption(OptionBuilder.isRequired(false).withArgName("spark app name").withLongOpt("spark-app-name").hasArg(true).withDescription("Spark Application Name").create("n"));
            options.addOption(OptionBuilder.isRequired(false).withArgName("key=value").withLongOpt("hive-properties").withValueSeparator('=').hasArgs().withDescription("Hive properties").create("p"));
            options.addOption(OptionBuilder.isRequired(false).withArgName("key=value").withLongOpt("hadoop-properties").withValueSeparator('=').hasArgs().withDescription("Hadoop properties").create("e"));
            options.addOption(OptionBuilder.isRequired(false).withLongOpt("help").hasArg(false).withDescription("Help").create("h"));
            GnuParser parser = new GnuParser();


            CommandLine commandLine = parser.parse(options, args);

            formatter = new HelpFormatter();

            String[] base = commandLine.getOptionValues("b");
            String[] delta = commandLine.getOptionValues("d");
            String[] output = commandLine.getOptionValues("o");
            if(base.length != 2 || delta.length != 2 || output.length != 2) {
                LOG.error("Base table, delta table and output table should be specified with database name qualified");
                return false;
            }
            baseDB = base[0];
            baseTable = base[1];
            deltaDB = delta[0];
            deltaTable = delta[1];
            outputDB = output[0];
            outputTable = output[1];
            joinKeys = Arrays.asList(commandLine.getOptionValue("k").split(","));
            localMode = commandLine.hasOption("l");
            sparkAppName = commandLine.getOptionValue("n", DEFAULT_APP_NAME);

            if(commandLine.hasOption("h")) {
                formatter.printHelp(DEFAULT_APP_NAME, options);
            }

            hiveConf = new HiveConf();
            if(commandLine.hasOption("p")) {
                commandLine.getOptionProperties("p").forEach((key, value) -> hiveConf.set(key.toString(), value.toString()));
            }
            hadoopConfiguration = new Configuration();
            if(commandLine.hasOption("a")) {
                hadoopConfiguration.addResource(new Path(commandLine.getOptionValue("a")));
            }
            if(commandLine.hasOption("e")) {
                commandLine.getOptionProperties("e").forEach((key, value) -> hadoopConfiguration.set(key.toString(), value.toString()));
            }
            if(commandLine.hasOption("s")) {
                hiveConf.addResource(new Path(commandLine.getOptionValue("s")));
            }

            return true;
        } catch (Exception ex) {
            LOG.error("Arguments are incorrect", ex);
            return false;
        }
    }
}
