package com.jasongj.spark;

import java.util.List;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkBucketJoin {

    private static final Logger LOG = LoggerFactory.getLogger(SparkBucketJoin.class);

    private static String baseTable;
    private static String deltaTable;
    private static List<String> joinKeys;
    private static int bucketNum;
    private static String sparkMode;
    private static String sparkAppName;

    public static void main(String[] args) throws ParseException {
        if(!parseArguments(args)) {
            return;
        }

    }

    private static boolean parseArguments(String[] args) {
        try{
            OptionGroup joinGroup = new OptionGroup();
            joinGroup.addOption(new Option("b", "base_table", true, "Base table name, including database name"));
            joinGroup.addOption(new Option("d","delta_table", true, "Incremental table name, including database name"));
            joinGroup.addOption((new Option("k", "join_key", true, "Join key")));
            joinGroup.setRequired(true);

            OptionGroup execGroup = new OptionGroup();
            execGroup.addOption(new Option("m", "spark_mode", true, "Spark execution mode"));
            execGroup.addOption(new Option("n", "spark_app_name", true, "Spark Application Name"));
            execGroup.setRequired(true);

            Options options = new Options();
            options.addOptionGroup(joinGroup).addOptionGroup(execGroup);
            CommandLineParser parser = new DefaultParser();
            CommandLine commandLine = parser.parse(options, args);

            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("SparkBucketJoin", options);

            return true;
        } catch (ParseException ex) {
            LOG.error("Arguments are incorrect", ex);
            return false;
        }
    }
}
