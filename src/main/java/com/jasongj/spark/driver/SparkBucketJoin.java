package com.jasongj.spark.driver;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import com.jasongj.spark.model.ExecutionEngineType;
import com.jasongj.spark.model.Tuple;
import com.jasongj.spark.reader.BucketReaderIterator;
import com.jasongj.spark.utils.FileSystemUtils;
import com.jasongj.spark.utils.HiveMetaDataExtractor;
import com.jasongj.spark.model.TableMetaData;
import com.jasongj.spark.join.SortMergeJoinIterator;
import com.jasongj.spark.writer.TupleWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
    private static HiveConf hiveConf;
    private static SparkConf sparkConf;
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

        Preconditions.checkArgument(baseTableMetaData.getExecutionEngineType() == deltaTableMetaData.getExecutionEngineType(),
                "Execution type of base table is %s while it of delta table is %s",
                baseTableMetaData.getExecutionEngineType(), deltaTableMetaData.getExecutionEngineType());

        outputTableMetaData.setExecutionEngineType(baseTableMetaData.getExecutionEngineType());

        if (!FileSystemUtils.checkAndLoadFile(hadoopConfiguration, baseTableMetaData)
                || !FileSystemUtils.checkAndLoadFile(hadoopConfiguration, deltaTableMetaData)
                || !FileSystemUtils.makeOutputTableReady(hadoopConfiguration, outputTableMetaData)) {
            return;
        }

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.hadoopConfiguration().addResource(hadoopConfiguration);


        Broadcast<TableMetaData> broadcastBaseTable = javaSparkContext.broadcast(baseTableMetaData);
        Broadcast<TableMetaData> broadcastDeltaTable = javaSparkContext.broadcast(deltaTableMetaData);
        Broadcast<TableMetaData> broadcastOutputTable = javaSparkContext.broadcast(outputTableMetaData);

        Map<String, String> hadoopConf = new ConcurrentHashMap<String, String>();
        hadoopConfiguration.forEach((Map.Entry<String, String> entry) -> hadoopConf.put(entry.getKey(), entry.getValue()));
        Broadcast<Map<String, String>> configurationBroadcast = javaSparkContext.broadcast(hadoopConf);

        List<Integer> list = IntStream.range(0, baseTableMetaData.getBucketNum()).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
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

            TableMetaData outputTable = broadcastOutputTable.value();
            Map<String, String> hadoopConfigurationMap = configurationBroadcast.value();
            Configuration configuration = new Configuration();
            hadoopConfigurationMap.forEach((String key, String value) -> configuration.set(key, value));
            FileSystem fileSystem = FileSystem.newInstance(configuration);

            TaskContext taskContext = TaskContext.get();
            int partitionIndex = taskContext.partitionId();
            int attemptNumber = taskContext.attemptNumber();
            Path path = getTmpPathName(outputTable, attemptNumber, partitionIndex);

            Class<? extends TupleWriter> tupleWriterClass = outputTable.getDataType().getWriterClass();
            TupleWriter tupleWriter = tupleWriterClass.getConstructor(Configuration.class, Path.class, TableMetaData.class).newInstance(configuration, path, outputTable);
            tupleWriter.init();
            iterator.forEachRemaining((Tuple tuple) -> tupleWriter.write(tuple));
            tupleWriter.close();

            Path finalPath = getBucketPathName(outputTable, partitionIndex);
            fileSystem.rename(path, finalPath);
            taskContext.addTaskCompletionListener((TaskContext context) -> {
                LOG.info("Write %s successfully with attempt number %d, partition id %d, attempt id %d", finalPath.getName(),
                        context.attemptNumber(), context.partitionId(), context.taskAttemptId());
            });
            taskContext.addTaskFailureListener((TaskContext context, Throwable throwable) -> {
                LOG.warn(String.format("Write %s failed with attempt number %d, partition id %d, attempt id %d", path.getName(),
                        context.attemptNumber(), context.partitionId(), context.taskAttemptId()), throwable);
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

    private static Path getTmpPathName(TableMetaData tableMetaData, int attemptNumber, int index) {
        return new Path(String.format("%s%s.%d.tmp", tableMetaData.getLocation().toString(), getBucketPathName(tableMetaData, index).getName(), attemptNumber));
    }

    private static Path getBucketPathName(TableMetaData tableMetaData, int index) {
        String path = StringUtils.EMPTY;
        if(tableMetaData.getExecutionEngineType() == ExecutionEngineType.TAZ) {
            path = String.format("%s/%s_0", tableMetaData.getLocation().toString(), StringUtils.leftPad(String.valueOf(index), 6, "0"));
        } else {
            path = String.format("%s/part-%s", tableMetaData.getLocation().toString(), StringUtils.leftPad(String.valueOf(index), 6, "0"));
        }
        return new Path(path);
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
            options.addOption(OptionBuilder.isRequired(false).withArgName("spark app name").withLongOpt("spark-app-name").hasArg(true).withDescription("Spark Application Name").create("n"));
            options.addOption(OptionBuilder.isRequired(false).withArgName("hive-site.xml").withLongOpt("hive-site").hasArg(true).withDescription("Hive configuration file. eg. hive-site.xml").create("s"));
            options.addOption(OptionBuilder.isRequired(false).withArgName("core-site.xml").withLongOpt("core-site").hasArg(true).withDescription("Hadoop configuration file. eg. core-site.xml").create("a"));
//            options.addOption(OptionBuilder.isRequired(false).withArgName("spark-defaults.properties").withLongOpt("spark-defaults").hasArg(true).withDescription("Spark property file. eg. spark-defaults.xml").create("c"));
            options.addOption(OptionBuilder.isRequired(false).withArgName("key=value").withLongOpt("hive-properties").withValueSeparator('=').hasArgs().withDescription("Hive properties").create("p"));
            options.addOption(OptionBuilder.isRequired(false).withArgName("key=value").withLongOpt("hadoop-properties").withValueSeparator('=').hasArgs().withDescription("Hadoop properties").create("e"));
            options.addOption(OptionBuilder.isRequired(false).withArgName("key=value").withLongOpt("spark-conf").withValueSeparator('=').hasArgs().withDescription("Spark configuration, which will override spark-defaults.xml").create("r"));
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
            boolean localMode = commandLine.hasOption("l");
            String sparkAppName = commandLine.getOptionValue("n", DEFAULT_APP_NAME);

            if(commandLine.hasOption("h")) {
                formatter.printHelp(sparkAppName, options);
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

            sparkConf = new SparkConf().setAppName(sparkAppName);
            if(localMode) {
                sparkConf.setMaster("local[3]");
            }
            if(commandLine.hasOption("r")) {
                commandLine.getOptionProperties("r").forEach((key, value) -> sparkConf.set(key.toString(), value.toString()));
            }

            return true;
        } catch (Exception ex) {
            LOG.error("Arguments are incorrect", ex);
            return false;
        }
    }
}
