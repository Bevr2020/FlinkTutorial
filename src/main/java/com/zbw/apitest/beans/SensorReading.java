package com.zbw.apitest.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author bevr
 * @create 2021-11-12 9:04
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SensorReading {
    private String id;
    private Long timestamp;
    private Double temperature;
}
