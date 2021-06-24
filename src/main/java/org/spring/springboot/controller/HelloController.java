package org.spring.springboot.controller;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.spring.springboot.entity.*;
import org.spring.springboot.response.Result;
import org.spring.springboot.service.HBaseUtils;
import org.spring.springboot.service.HbaseClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

@RestController
public class HelloController {

    public static org.apache.hadoop.conf.Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    @Autowired
    private HBaseUtils hbaseUtils;
    private HbaseClient hbaseClient = new HbaseClient();
    @GetMapping("/getVideoTable")
    public Result getVideoTable(@RequestParam("videoId") String videoId)
    {
        Double duration = hbaseUtils.getDoubleData("video",videoId,"info","duration");
//        System.out.println(hbaseUtils.getDoubleData("video",videoId,"info","duration"));
        if(duration == -1){
            return Result.error().data("提示","该videoId不存在相关信息");
        }
        Video video = new Video();
        video.setDuration(duration);
        video.setVideoId(videoId);
        return  Result.ok().data("video",video);
    }

    @GetMapping("/getProblemTable")
    public Result getProblemTable(@RequestParam("problemId") String problemId)
    {
        String data = hbaseUtils.getData("problem", problemId, "times", "all");
        if(data.equals("") || data ==null){
            return Result.error().data("提示","该problemId不存在相关信息");
        }
        Double all = Double.valueOf(hbaseUtils.getData("problem",problemId,"times","all"));
        Double right = Double.valueOf(hbaseUtils.getData("problem",problemId,"times","right"));
        Double wrong = Double.valueOf(hbaseUtils.getData("problem",problemId,"times","wrong"));
        String concept = hbaseUtils.getData("problem",problemId,"info","concept");
        concept = concept.substring(2, concept.length() - 2);
        String preConcept = hbaseUtils.getData("problem",problemId,"info","preConcept");
        Problem problem = new Problem();
        problem.setProblemId(problemId);
        problem.setAll(all);
        problem.setRight(right);
        problem.setWrong(wrong);
        problem.setConcept(concept);
        problem.setPreConcept(preConcept);
        return Result.ok().data("problem",problem);
    }

    @GetMapping("/getStudentTable")
    public Result getStudentTable(@RequestParam("studentId") String studentId)
    {
        String conceptGrasp = hbaseUtils.getData("student",studentId,"info","conceptGrasp");
        if(conceptGrasp.equals("") || conceptGrasp== null){
            return Result.error().data("提示","该studentId不存在相关信息");
        }
        String conceptAlmostGrasp = hbaseUtils.getData("student",studentId,"info","conceptAlmostGrasp");
        String conceptNoGrasp = hbaseUtils.getData("student",studentId,"info","conceptNoGrasp");
        Double all = Double.valueOf(hbaseUtils.getData("student",studentId,"info","all"));
        Double right =  Double.valueOf(hbaseUtils.getData("student",studentId,"info","right"));
        String s1 = conceptNoGrasp.replaceAll("\\\\","");
        String s2 = s1.replaceAll("\\\"","");
        Student student = new Student();
        String s3 = s2.replaceAll(","," ");
        if(conceptGrasp.equals("") || conceptGrasp == null){
            student.setCountOfGrasp(0);
        }else{
            Integer count = conceptGrasp.split(" ").length;
            student.setCountOfGrasp(count);
        }
        if(conceptNoGrasp.equals("") || conceptNoGrasp == null){
            student.setCountOfNoGrasp(0);
        }else{
            Integer count = conceptNoGrasp.split(" ").length;
            student.setCountOfNoGrasp(count);
        }
        if(conceptAlmostGrasp.equals("") || conceptAlmostGrasp == null){
            student.setCountOfAlmostGrasp(0);
        }else{
            Integer count = conceptAlmostGrasp.split(" ").length;
            student.setCountOfAlmostGrasp(count);
        }
        student.setStudentId(studentId);
        student.setConceptGrasp(conceptGrasp);
        student.setConceptAlmostGrasp(conceptAlmostGrasp);
        student.setConceptNoGrasp(s3);
        student.setAll(all);
        student.setRight(right);
        return Result.ok().data("student",student);
    }
    @GetMapping("/getCourseTable")
    public Result getCourseTable(@RequestParam("courseId") Integer courseId)
    {
        Course course = new Course();

        List<String> list = new ArrayList<>();
        list.add("course-v1:JXUST+JXUST2016001+2016_T2");
        list.add("course-v1:TsinghuaX+30240243X+sp");
        list.add("course-v1:UST+UST001+sp");
        list.add("course-v1:HBNU+2019051509X+2019_T1");

        list.add("course-v1:TsinghuaX+70240403+2019_T1");
        list.add("course-v1:TsinghuaX+30240184+sp");
        list.add("course-v1:TsinghuaX+00740043X_2015_T2+sp");
        list.add("course-v1:TsinghuaX+00740043_2x_2015_T2+sp");

        list.add("course-v1:TsinghuaX+00740123_X+sp");
        list.add("course-v1:SCUT+2018122802X+2018_T2");
        list.add("course-v1:TsinghuaX+30240184_2X+sp");
        list.add("course-v1:MITx+6_00_1x+sp");

        HashMap<Integer, String> map = new HashMap<>();
        Integer i = 0;
        for (String s : list)
        {
            map.put(i++,s);
        }
        String a = map.get(courseId);
        course.setCourseId(a);
        String svideoCount = Bytes.toString(hbaseClient.get(TableName.valueOf("course"), Bytes.toBytes(a), Bytes.toBytes("info"), Bytes.toBytes("videoCount")));
        if( svideoCount!=null){
            course.setVideoCount(Double.valueOf(svideoCount));
        }else{
            return Result.error().data("提示","不存在该课程");
        }
        String svideoDuration = Bytes.toString(hbaseClient.get(TableName.valueOf("course"), Bytes.toBytes(a), Bytes.toBytes("info"), Bytes.toBytes("videoDuration")));
        if( svideoDuration!=null){
            course.setVideoDuration(Double.valueOf(svideoDuration));
        }else{
            course.setVideoDuration(0.0);
        }

        String scount = Bytes.toString(hbaseClient.get(TableName.valueOf("course"), Bytes.toBytes(a), Bytes.toBytes("info"), Bytes.toBytes("count")));
        if( scount!=null){
            course.setCount(Double.valueOf(scount));
        }else{
            course.setCount(0.0);
        }

        String sallTimes = Bytes.toString(hbaseClient.get(TableName.valueOf("course"), Bytes.toBytes(a), Bytes.toBytes("info"), Bytes.toBytes("allTimes")));
        if( sallTimes!=null){
            course.setAllTimes(Double.valueOf(sallTimes));
        }else{
            course.setAllTimes(0.0);
        }

        String srightTimes = Bytes.toString(hbaseClient.get(TableName.valueOf("course"), Bytes.toBytes(a), Bytes.toBytes("info"), Bytes.toBytes("rightTimes")));
        if( srightTimes!=null){
            course.setRightTimes(Double.valueOf(srightTimes));
        }else{
            course.setRightTimes(0.0);
        }
        return Result.ok().data("course",course);
    }
    @GetMapping("/getAllCourseTable")
    public Result getAllCourseTable()
    {
        List<String> list = new ArrayList<>();
        list.add("course-v1:JXUST+JXUST2016001+2016_T2");
        list.add("course-v1:TsinghuaX+30240243X+sp");
        list.add("course-v1:UST+UST001+sp");
        list.add("course-v1:HBNU+2019051509X+2019_T1");

        list.add("course-v1:TsinghuaX+70240403+2019_T1");
        list.add("course-v1:TsinghuaX+30240184+sp");
        list.add("course-v1:TsinghuaX+00740043X_2015_T2+sp");
        list.add("course-v1:TsinghuaX+00740043_2x_2015_T2+sp");

        list.add("course-v1:TsinghuaX+00740123_X+sp");
        list.add("course-v1:SCUT+2018122802X+2018_T2");
        list.add("course-v1:TsinghuaX+30240184_2X+sp");
        list.add("course-v1:MITx+6_00_1x+sp");
        ArrayList<Course> courses = new ArrayList<>();
        for (String s: list){
            Course course = new Course();
            course.setCourseId(s);
            String svideoCount = Bytes.toString(hbaseClient.get(TableName.valueOf("course"), Bytes.toBytes(s), Bytes.toBytes("info"), Bytes.toBytes("videoCount")));
            if( svideoCount!=null){
                course.setVideoCount(Double.valueOf(svideoCount));
            }else{
                course.setVideoCount(0.0);
            }

            String svideoDuration = Bytes.toString(hbaseClient.get(TableName.valueOf("course"), Bytes.toBytes(s), Bytes.toBytes("info"), Bytes.toBytes("videoDuration")));
            if( svideoDuration!=null){
                course.setVideoDuration(Double.valueOf(svideoDuration));
            }else{
                course.setVideoDuration(0.0);
            }

            String scount = Bytes.toString(hbaseClient.get(TableName.valueOf("course"), Bytes.toBytes(s), Bytes.toBytes("info"), Bytes.toBytes("count")));
            if( scount!=null){
                course.setCount(Double.valueOf(scount));
            }else{
                course.setCount(0.0);
            }

            String sallTimes = Bytes.toString(hbaseClient.get(TableName.valueOf("course"), Bytes.toBytes(s), Bytes.toBytes("info"), Bytes.toBytes("allTimes")));
            if( sallTimes!=null){
                course.setAllTimes(Double.valueOf(sallTimes));
            }else{
                course.setAllTimes(0.0);
            }

            String srightTimes = Bytes.toString(hbaseClient.get(TableName.valueOf("course"), Bytes.toBytes(s), Bytes.toBytes("info"), Bytes.toBytes("rightTimes")));
            if( srightTimes!=null){
                course.setRightTimes(Double.valueOf(srightTimes));
            }else{
                course.setRightTimes(0.0);
            }
            courses.add(course);
        }
        return Result.ok().data("course",courses);

    }

    @GetMapping("/getAllVideoTable")
    public Result getAllVideoTable(@RequestParam("current") Integer current,
                                   @RequestParam("size") Integer size)
    {
        ArrayList<Video> videos = new ArrayList<>();
        List<Map<String, String>> video = hbaseUtils.getAllVideoTable("video");
        int i = 0;
        for (Map<String,String> map: video){
            Video video1 = new Video();
            for (Map.Entry<String, String> entry : map.entrySet())
            {
                if(i % 2 == 0){
                    video1.setVideoId(entry.getValue());
                }else{
                    video1.setDuration(Double.valueOf(entry.getValue()));
                    videos.add(video1);
                }
                i++;
            }
        }
        ArrayList<Video> arrayList = new ArrayList<>();
        int count = 0;
        for (Video answer: videos)
        {
            if((count>=(current*size)) && (count <(current*size+size))){
                arrayList.add(answer);
            }
            if(count >= (current*size+size)){
                break;
            }
            count++;
        }
        return Result.ok().data("total",videos.size()).data("videos",arrayList);
    }
    @GetMapping("/getAllProblemTable")
    public Result getAllProblemTable(@RequestParam("current") Integer current,
                                     @RequestParam("size") Integer size)
    {
        ArrayList<ProblemVo> problemVos = new ArrayList<>();
        ArrayList<Problem> problems = new ArrayList<>();
        List<Map<String, String>> problemTable = hbaseUtils.getData("problem");
        for (Map<String,String> map: problemTable)
        {

            if(map.size() == 2) {
                int i = 0;
                ProblemVo problemVo = new ProblemVo();
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    if(i % 2 == 0){
                        problemVo.setPreConcept(entry.getValue());
                    }else{
                        problemVo.setProblemId(entry.getValue());
                        problemVos.add(problemVo);
                    }
                    i++;
                }
            }
            if(map.size() == 5)
            {
                int i = 1;
                Problem problem = new Problem();
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    if(i % 5 == 1){
                        problem.setAll(Double.valueOf(entry.getValue()));
                    }
                    else if (i % 5 == 2){
                        problem.setConcept(entry.getValue().substring(2, entry.getValue().length() - 2));
                    }
                    else if (i % 5 == 3){
                        problem.setProblemId(entry.getValue());
                    }
                    else if (i % 5 == 4){
                        problem.setWrong(Double.valueOf(entry.getValue()));
                    }
                    else{
                        problem.setRight(Double.valueOf(entry.getValue()));
                        problems.add(problem);
                    }
                    i++;
                }
            }
        }
        ArrayList<Problem> pageProblem = new ArrayList<>();
        int count = 0;
        for (Problem problem: problems)
        {
            if(count>=(current*size) && count <(current*size+size)){
                pageProblem.add(problem);
            }
            if(count >= (current*size+size)){
                break;
            }
            count++;
        }
        return Result.ok().data("totalOfProblem",problems.size()).data("problems",pageProblem);
//        return Result.ok().data("all",problemTable);
    }

    @GetMapping("/getAllStudentTable")
    public Result getAllStudentTable(@RequestParam("current") Integer current,
                                     @RequestParam("size") Integer size) {
        List<Map<String, String>> studentTable = hbaseUtils.getData("student");
        ArrayList<Student> students = new ArrayList<>();
        for (Map<String,String> map: studentTable)
        {
            if(map.size() == 6)
            {
                int i = 1;
                Student student = new Student();
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    if(i % 6 == 1){
                        student.setRight(Double.valueOf(entry.getValue()));
                    }
                    else if (i % 6 == 2){
                        if(entry.getValue().equals("") || entry.getValue() == null){
                            student.setCountOfGrasp(0);
                        }else{
                            Integer count = entry.getValue().split(" ").length;
                            student.setCountOfGrasp(count);
                        }
                        student.setConceptGrasp(entry.getValue());
                    }
                    else if (i % 6 == 3){
                        if(entry.getValue().equals("") || entry.getValue() == null){
                            student.setCountOfNoGrasp(0);
                        }else{
                            Integer count = entry.getValue().split(" ").length;
                            student.setCountOfNoGrasp(count);
                        }
                        String s1 = entry.getValue().replaceAll("\\\\","");
                        String s2 = s1.replaceAll("\\\"","");
                        String s3 = s2.replaceAll(","," ");
                        student.setConceptNoGrasp(s3);
                    }
                    else if (i % 6 == 4){
                        if(entry.getValue().equals("") || entry.getValue() == null){
                            student.setCountOfAlmostGrasp(0);
                        }else{
                            Integer count = entry.getValue().split(" ").length;
                            student.setCountOfAlmostGrasp(count);
                        }
                        student.setConceptAlmostGrasp(entry.getValue());
                    }
                    else if( i % 6  == 5){
                        student.setAll(Double.valueOf(entry.getValue()));
                    }
                    else {
                        student.setStudentId(entry.getValue());
                        students.add(student);
                    }
                    i++;
                }
            }
        }
        ArrayList<Student> pageStudent = new ArrayList<>();
        int count = 0;
        for (Student student: students)
        {
            if(count>=(current*size) && count <(current*size+size)){
                pageStudent.add(student);
            }
            if(count >= (current*size+size)){
                break;
            }
            count++;
        }
        return Result.ok().data("total",students.size()).data("students",pageStudent);
    }

    @GetMapping("/judge")
    public Result judge(@RequestParam("studentId") String studentId,
                        @RequestParam("problemId") String problemId)
    {
//        Double allStudent = Double.valueOf(hbaseUtils.getData("student",studentId,"info","all"));
        String data = hbaseUtils.getData("student", studentId, "info", "all");
        if (data == null || data.equals("")){
            return Result.error().data("提示","该studentId不存在相关信息");
        }
        Double allStudent = Double.valueOf(hbaseUtils.getData("student",studentId,"info","all"));
        Student student  = (Student) getStudentTable(studentId).getData().get("student");
        String data1 = hbaseUtils.getData("problem", problemId, "times", "all");
        if(data1 == null || data1.equals("")){
            return Result.error().data("提示","该problemId不存在相关信息");
        }
        Double allProblem = Double.valueOf(data1);
        Problem problem = (Problem) getProblemTable(problemId).getData().get("problem");
        Double problemConceptRightRate = 0.0;
        Double studentRightRate = student.getRight()/student.getAll();
        Double problemRightRate = problem.getRight()/problem.getAll();
        Random r = new Random();
        for (int i = 1 ; i<= 3 ; i++)
        {
            if(Arrays.asList(student.getConceptGrasp().split(" ")).contains(problem.getConcept())){
                problemConceptRightRate = r.nextDouble()*0.2+0.8;
                break;
            }
            else if (Arrays.asList(student.getConceptAlmostGrasp().split(" ")).contains(problem.getConcept())){
                problemConceptRightRate = r.nextDouble()*0.1+0.5;
            }else {
                problemConceptRightRate = r.nextDouble()*0.4+0.1;
            }
        }
        problemConceptRightRate = Double.valueOf(String.format("%.2f", problemConceptRightRate));
        studentRightRate = Double.valueOf(String.format("%.2f", studentRightRate));
        problemRightRate = Double.valueOf(String.format("%.2f", problemRightRate));
        Double rightRate = Double.valueOf(String.format("%.3f",problemConceptRightRate*studentRightRate*problemRightRate ));
        Double errorRate = Double.valueOf(String.format("%.3f",(1-problemConceptRightRate)*(1-studentRightRate+0.5)*(1-problemRightRate+0.5) ));
//        Double errorRate = (1-problemConceptRightRate)*(1-studentRightRate)*(1-problemRightRate);
        String answer = "";
        if(rightRate >= errorRate){
            answer = "正确";
        }else {
            answer = "错误";
        }
        Double rate = (rightRate >= errorRate)? rightRate:errorRate;
        String analyse = "";
        if(rate >= 0.7)
        {
            analyse = "您的答题正确率很高,";
            if(problemConceptRightRate >= 0.8){
                analyse += "因为已经掌握 "+problem.getConcept()+" 知识点";
            }else if (problemConceptRightRate >= 0.5){
                analyse += "基本掌握 "+problem.getConcept()+" 知识点";
            }else {
                analyse += "但您尚未掌握 "+problem.getConcept()+" 知识点";
            }
        }
        else if(rate >= 0.4)
        {
            analyse = "您的答题正确率较高,";
            if(problemConceptRightRate >= 0.8){
                analyse += "因为已经掌握 "+problem.getConcept()+" 知识点";
            }else if (problemConceptRightRate >= 0.5){
                analyse += "基本掌握 "+problem.getConcept()+" 知识点";
            }else {
                analyse += "但您尚未掌握 "+problem.getConcept()+" 知识点";
            }
        }
        else if(rate >= 0.0)
        {
            analyse = " 您的答题正确率略低,";
            if(problemConceptRightRate >= 0.8){
                analyse += "但您已经掌握 "+problem.getConcept()+" 知识点";
            }else if (problemConceptRightRate >= 0.5){
                analyse += "基本掌握 "+problem.getConcept()+" 知识点";
            }else {
                analyse += "尚未掌握 "+problem.getConcept()+" 知识点";
            }
        }
        String s1 = student.getConceptGrasp().replaceAll("\\\\","");
        String s2 = s1.replaceAll("\\\"","");
        String s3 = s2.replaceAll(","," ");
        String problemDetail = "该问题回答正确率为 "+problemRightRate+",涉及知识点是"+problem.getConcept()+"。";
        String studentDetail = "掌握知识点为"+s3+"。";
        analyse = "该学生总答题次数为："+student.getAll()+"。该学生答题正确率为 "+studentRightRate+"。"+problemDetail+analyse;
        return Result.ok().data("answer",answer).data("rightRate",rightRate).data("errorRate",errorRate).data("studentDetail",studentDetail).data("analyse",analyse);
    }

}
