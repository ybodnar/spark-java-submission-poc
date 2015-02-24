package com.ybodnar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.apache.spark.examples.SparkPi;
import org.apache.spark.deploy.SparkSubmit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class SparkSubmitService {
    public static void main(String[] args) throws IOException, YarnException {
        new SparkSubmitService().submit();
    }
    public void submit() throws IOException, YarnException {

//        Configuration configuration = new YarnConfiguration(new Configuration());
//        YarnClient yarnClient = YarnClient.createYarnClient();
//        yarnClient.init(configuration);
//        yarnClient.start();

        Configuration configuration = new Configuration();
        SparkConf sparkConf = new SparkConf().setMaster("yarn-client");
        System.out.println(sparkConf);
        Client sparkClient = new Client(new ClientArguments(getSparkPiExampleArgs(), sparkConf));
//        sparkClient.submitApp();
        sparkClient.submitApplication();
    }

    private String[] getSparkPiExampleArgs() {

        List<String> argumentsList = new ArrayList<>();
        argumentsList.add("--class");
        argumentsList.add(SparkPi.class.getCanonicalName());
        argumentsList.add("--jar");
        argumentsList.add("/Users/yuriybodnar/Devel/spark-submit/target/spark-submit-1.0-SNAPSHOT-jar-with-dependencies.jar");
//        argumentsList.add("/Users/yuriybodnar/.m2/repository/org/apache/spark/spark-examples_2.10/1.1.1/spark-examples_2.10-1.1.1.jar");
        argumentsList.add("--name");
        argumentsList.add("YarnPiTest");
        argumentsList.add("--driver-memory");
        argumentsList.add("256m");
        argumentsList.add("--executor-memory");
        argumentsList.add("256m");
        argumentsList.add("--num-executors");
        argumentsList.add("1");
        argumentsList.add("--executor-cores");
        argumentsList.add("1");
//        argumentsList.add("--addJars");
//        argumentsList.add("/user/yuriybodnar/spark-submit-1.0-SNAPSHOT-jar-with-dependencies.jar");


//        argumentsList.add("--master");
//        argumentsList.add("yarn-cluster");
        return argumentsList.toArray(new String[argumentsList.size()]);
    }
}
