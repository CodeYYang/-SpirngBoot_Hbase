package org.spring.springboot.entity;

import lombok.Data;

@Data
public class Course {
    private String courseId;
    private Double videoCount;
    private Double videoDuration;
    private Double count;
    private Double allTimes;
    private Double rightTimes;
}
