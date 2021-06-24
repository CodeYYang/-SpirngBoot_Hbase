package org.spring.springboot.entity;

import lombok.Data;

@Data
public class Student {
    private String studentId;
    private String conceptGrasp;
    private String conceptAlmostGrasp;
    private String conceptNoGrasp;
    private Double all;
    private Double right;
    private Integer countOfGrasp;
    private Integer countOfAlmostGrasp;
    private Integer countOfNoGrasp;
}
