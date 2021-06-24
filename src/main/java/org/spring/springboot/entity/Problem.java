package org.spring.springboot.entity;

import lombok.Data;

@Data
public class Problem {
    private String problemId;
    private Double all;
    private Double right;
    private Double wrong;
    private String concept;
    private String preConcept;
}
