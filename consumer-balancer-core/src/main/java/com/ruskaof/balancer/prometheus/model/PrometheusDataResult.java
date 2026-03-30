package com.ruskaof.balancer.prometheus.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PrometheusDataResult {
    private Map<String, String> metric;
    private InstantValue value;
}
