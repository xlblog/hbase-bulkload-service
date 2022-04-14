package com.allsenseww.hbase.service.bulkload;

import java.io.Serializable;
import java.util.NavigableMap;

public class HBaseResult implements Serializable {
    private byte[] row;
    private NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map;

    public byte[] getRow() {
        return row;
    }

    public void setRow(byte[] row) {
        this.row = row;
    }

    public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getMap() {
        return map;
    }

    public void setMap(NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map) {
        this.map = map;
    }
}
