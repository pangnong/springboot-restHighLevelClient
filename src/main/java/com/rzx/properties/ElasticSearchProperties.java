package com.rzx.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author wuyue
 * @date 2021/12/22 9:48
 */
@Data
@ConfigurationProperties(prefix = "elasticsearch")
public class ElasticSearchProperties {
    /**
     * Is enable
     */
    private boolean enable = false;

    /**
     * elasticsearch server host
     */
    private String host;

    /**
     * elasticsearch server port
     */
    private int port;

    /**
     * scheme
     */
    private String scheme = "http";
}
