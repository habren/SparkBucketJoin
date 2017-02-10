package com.jasongj.spark.utils;

import com.jasongj.spark.model.Bucket;
import com.jasongj.spark.model.ExecutionEngineType;
import com.jasongj.spark.model.TableMetaData;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Jason Guo (jason.guo.vip@gmail.com)
 */
public class FileSystemUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FileSystemUtils.class);

    public static boolean makeOutputTableReady(Configuration hadoopConfiguration, TableMetaData tableMetaData) {
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

    public static boolean checkAndLoadFile(Configuration hadoopConfiguration, TableMetaData tableMetaData) {
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
                    tableMetaData.setExecutionEngineType(ExecutionEngineType.TAZ);
                    return new Bucket(filePath.toUri(), index, fileStatus.getLen());
                }

                // Extract bucket sequence from Map Reduce based hive table bucket file name
                Pattern bucketFileMRPattern = Pattern.compile("part-(\\d+)");
                Matcher matcherMR = bucketFileMRPattern.matcher(baseName);
                if(matcherMR.matches()) {
                    int index = Integer.parseInt(matcherMR.group(1));
                    tableMetaData.setExecutionEngineType(ExecutionEngineType.MAP_REDUCE);
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
}
