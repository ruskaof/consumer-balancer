package com.ruskaof.balancer.prometheus.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PrometheusData {
    private List<PrometheusDataResult> result;
}
