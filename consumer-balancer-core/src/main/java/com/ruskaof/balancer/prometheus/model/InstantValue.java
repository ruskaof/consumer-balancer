package com.ruskaof.balancer.prometheus.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonFormat(shape = JsonFormat.Shape.ARRAY)
public class InstantValue {
    private double timestamp;
    private String value;
}
