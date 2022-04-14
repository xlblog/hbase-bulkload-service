package com.allsenseww.hbase.service.pojo;

import lombok.Data;

@Data
public class BulkloadBatchBean {

    private String sourceTableName;
    private String targetTableName;
    private Long startTime;
    private Long endTime;
    private Integer parallelism;
    private Boolean rollup;
    private Long timeInterval;

}
