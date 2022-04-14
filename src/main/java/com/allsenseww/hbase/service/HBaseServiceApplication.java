package com.allsenseww.hbase.service;

import com.allsenseww.hbase.service.configuration.HBaseProperties;
import com.allsenseww.hbase.service.configuration.OssProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication(scanBasePackages = {"com.allsenseww.hbase.service"})
@EnableConfigurationProperties({HBaseProperties.class, OssProperties.class})
public class HBaseServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(HBaseServiceApplication.class);
    }

}
