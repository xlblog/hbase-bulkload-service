package com.allsenseww.hbase.service.bulkload;

import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;

public class HFileOutputUtil {

    public static HadoopOutputFormat2 buildHadoopOutputFormat(String zkQuorum, String tableName, String outputPath) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
        conf.setInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 2048);
        conf.set("dfs.replication", "1");
        //设置Hfile压缩
        conf.set("hfile.compression", "snappy");
        conf.set("hbase.defaults.for.version.skip", "true");
        conf.set("hbase.regionserver.codecs", "snappy");
        conf.set("hbase.mapreduce.hfileoutputformat.table.name", tableName);

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Job job = Job.getInstance(conf);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        HFileOutputFormat2 hFileOutputFormat2 = new HFileOutputFormat2();
        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(TableName.valueOf(tableName)));
        HFileOutputFormat2.setOutputPath(job, new Path(outputPath));

        return new HadoopOutputFormat2(hFileOutputFormat2, job);
    }

    public static HadoopOutputFormat3 buildHadoopOutputFormat3(String zkQuorum, String tableName, String outputPath) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
        conf.setInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 2048);
        conf.set("dfs.replication", "1");
        //设置Hfile压缩
        conf.set("hfile.compression", "snappy");
        conf.set("hbase.defaults.for.version.skip", "true");
        conf.set("hbase.regionserver.codecs", "snappy");
        conf.set("hbase.mapreduce.hfileoutputformat.table.name", tableName);

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Job job = Job.getInstance(conf);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        HFileOutputFormat2 hFileOutputFormat2 = new HFileOutputFormat2();
        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(TableName.valueOf(tableName)));
        HFileOutputFormat2.setOutputPath(job, new Path(outputPath));

        return new HadoopOutputFormat3(hFileOutputFormat2, job);
    }

    public static org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat<ImmutableBytesWritable, Cell> buildRollupHadoopOutputFormat(String zkQuorum, String tableName, String outputPath) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
        conf.setInt(org.apache.hadoop.hbase.tool.LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 2048);
        conf.set("dfs.replication", "1");
        //设置Hfile压缩
        conf.set("hfile.compression", "snappy");
        conf.set("hbase.defaults.for.version.skip", "true");
        conf.set("hbase.regionserver.codecs", "snappy");
        conf.set("hbase.mapreduce.hfileoutputformat.table.name", tableName);

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Job job = Job.getInstance(conf);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        HFileOutputFormat2 hFileOutputFormat2 = new HFileOutputFormat2();
        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(TableName.valueOf(tableName)));
        HFileOutputFormat2.setOutputPath(job, new Path(outputPath));

        return new HadoopOutputFormat<>(hFileOutputFormat2, job);
    }

}
