package com.allsenseww.hbase.service.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.service.oss")
@Data
public class OssProperties {

    private String ak;
    private String secret;
    private String bucket;
    private String endpoint;
    private String basePath;

}
