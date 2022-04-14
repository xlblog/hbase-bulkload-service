package com.allsenseww.hbase.service.pojo;

import lombok.Data;

import java.io.Serializable;

@Data
public class BulkloadBean implements Serializable {

    private String sourceTableName;
    private String targetTableName;
    private Long startTime;
    private Long endTime;
    private Integer parallelism;
    private Boolean rollup;

}
