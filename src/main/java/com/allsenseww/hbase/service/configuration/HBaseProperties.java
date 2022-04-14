package com.allsenseww.hbase.service.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.Serializable;

@ConfigurationProperties(prefix = "app.service.hbase")
@Data
public class HBaseProperties implements Serializable {

    private String nameServer;
    private String zkQuorum;
    private String zkPort = "2181";
    private String tableName;
    private Integer bucketCount = 512;
    private Long upperSize = 5 * 1024 * 1024 * 1024L;
    private Long lowerSize = 100 * 1024L * 1024;
    private Long longTimeInterval = 3 * 365 * 24 * 60 * 60 * 1000L;     //3年
    private Long shortTimeInterval = 3 * 30 * 24 * 60 * 60 * 1000L;     //3月

    private String hdfsRootDir;
    private String repairTableName;
    private String rollupTableName = "bucket-tsdb-rollup-5m";

}
