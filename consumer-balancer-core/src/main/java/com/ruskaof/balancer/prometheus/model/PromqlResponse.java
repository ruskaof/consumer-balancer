package com.ruskaof.listener.prometheus.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PromqlResponse {
    private String status;
    private PrometheusData data;
}

