package com.allsenseww.hbase.service.bulkload;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.hadoop.hbase.HConstants;

public class HBaseOptions {

    public static final ConfigOption<String> TABLE_NAME_OPTION = ConfigOptions.key("tableName")
            .stringType()
            .noDefaultValue()
            .withDescription("HBase Table Name");

    public static final ConfigOption<String> ZOOKEEPER_QUORUM_OPTION = ConfigOptions.key(HConstants.ZOOKEEPER_QUORUM)
            .stringType()
            .noDefaultValue()
            .withDescription("Zookeeper服务地址");

    public static final ConfigOption<String> ZOOKEEPER_CLIENT_PORT_OPTION = ConfigOptions.key(HConstants.ZOOKEEPER_CLIENT_PORT)
            .stringType()
            .defaultValue("2181")
            .withDescription("Zookeeper 端口号");

    public static final ConfigOption<Integer> ZOOKEEPER_TIMEOUT_OPTION = ConfigOptions.key("timeout")
            .intType()
            .defaultValue(24000)
            .withDescription("HBase 超时时间");

    public static final ConfigOption<Long> START_OPTION = ConfigOptions.key("startTime")
            .longType()
            .noDefaultValue()
            .withDescription("scan start time");

    public static final ConfigOption<Long> END_OPTION = ConfigOptions.key("endTime")
            .longType()
            .noDefaultValue()
            .withDescription("scan end time");
}
