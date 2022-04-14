package com.allsenseww.hbase.service.bulkload;

import lombok.Data;

@Data
public class BulkloadResult {

    private String jobId;
    private String outputPath;
    private String targetTable;
    private String startTime;
    private String endTime;
    private boolean autoLoad;

}
