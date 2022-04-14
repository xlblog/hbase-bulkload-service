package com.allsenseww.hbase.service.manager;

import com.allsenseww.hbase.service.configuration.HBaseProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class HBaseConnectionManager {

    @Autowired
    private HBaseProperties hBaseProperties;

    public Connection getConnection() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", hBaseProperties.getZkQuorum());
        configuration.set("hbase.zookeeper.property.clientPort", hBaseProperties.getZkPort());
        try {
            return ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            throw new RuntimeException("create hbase connection failed.", e);
        }
    }

    public void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                log.error("close hbase connection failed.", e);
            }
        }
    }

}
