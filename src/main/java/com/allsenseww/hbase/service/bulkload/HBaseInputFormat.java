package com.allsenseww.hbase.service.bulkload;

import org.apache.flink.connector.hbase2.source.AbstractTableInputFormat;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.io.Serializable;

public class HBaseInputFormat extends AbstractTableInputFormat<HBaseResult> implements Serializable {

    private final String tableName;
    private final Long startTime;
    private final Long endTime;

    public HBaseInputFormat(String tableName, Long startTime, Long endTime, org.apache.hadoop.conf.Configuration hConf) {
        super(hConf);
        this.tableName = tableName;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    protected void initTable() throws IOException {
        try {
            Connection connection = ConnectionFactory.createConnection(getHadoopConfiguration());
            TableName name = TableName.valueOf(this.tableName);
            this.table = connection.getTable(name);
            regionLocator = connection.getRegionLocator(name);
            this.scan = TsdbScanner.buildScan(startTime, endTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected Scan getScanner() {
        return this.scan;
    }

    protected String getTableName() {
        return this.tableName;
    }

    protected HBaseResult mapResultToOutType(Result result) {
        HBaseResult hBaseResult = new HBaseResult();
        hBaseResult.setRow(result.getRow());
        hBaseResult.setMap(result.getMap());
        return hBaseResult;
    }

}
