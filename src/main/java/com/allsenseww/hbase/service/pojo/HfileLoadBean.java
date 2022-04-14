package com.allsenseww.hbase.service.pojo;

import lombok.Data;

import java.util.List;

@Data
public class HfileLoadBean {

    private List<String> outputPaths;
    private String targetTableName;

}
