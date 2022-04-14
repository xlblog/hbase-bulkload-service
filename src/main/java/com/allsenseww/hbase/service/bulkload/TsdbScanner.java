package com.allsenseww.hbase.service.bulkload;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TsdbScanner implements Serializable {

    private static final Long HOUR_MILL = 3600L * 1000L;

    public static Scan buildScan(Long startTime, Long endTime) {
        byte[] filterBytes = new byte[]{0, 0, 0, 0, 0, 0, 0};
        long startIntactTime = getLastHourTime(startTime);
        long endIntactTime = getLastHourTime(endTime);

        List<Pair<byte[], byte[]>> pairList = new ArrayList<>();
        while (startIntactTime < endIntactTime) {
            // tmp 临时变量，拷贝于filterbytes，区别于时间。用于构建filter
            byte[] tmp = new byte[filterBytes.length];
            System.arraycopy(filterBytes, 0, tmp, 0, filterBytes.length);

            // 重新设置 tmp 时间
            byte[] startTimeBytes = Bytes.toBytes(startIntactTime / 1000);
            System.arraycopy(startTimeBytes, 4, tmp, 3, 4);

            byte[] pairBytes = new byte[tmp.length];
            for (int i = 0; i < pairBytes.length; i++) {
                if (i < 3) {
                    pairBytes[i] = 1;
                } else {
                    pairBytes[i] = 0;
                }
            }
            Pair<byte[], byte[]> pair = new Pair<>(tmp, pairBytes);
            pairList.add(pair);

            startIntactTime += HOUR_MILL;
        }

//            return new Scan();

        return new Scan()
                .setRaw(false)
                .setSmall(true)
                .setFilter(
                        new FuzzyRowFilter(pairList)
                );
    }

    private static Long getLastHourTime(Long time) {
        return time - time % HOUR_MILL;
    }

}
