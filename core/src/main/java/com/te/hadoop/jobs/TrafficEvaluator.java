package com.te.hadoop.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.math.Ordering;

import javax.xml.crypto.dsig.keyinfo.KeyInfo;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;


/**
 * Created by IntelliJ IDEA.
 * User: smishra
 * Date: 6/27/12
 * Time: 9:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class TrafficEvaluator extends Configured implements Tool {
    private static final Logger logger = LoggerFactory.getLogger(TrafficEvaluator.class);

    @java.lang.Override
    public int run(java.lang.String[] strings) throws Exception {
        Configuration conf =this.getConf();
        conf.setQuietMode(false);

        Job job = new Job(conf, "Traffic Eye");

        job.setJarByClass(this.getClass());

        job.setMapperClass(TrafficEvaluatorMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(TrafficEvaluatorReducer.class);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TrafficEvaluator(), args);
        System.exit(res);
    }
}

class TrafficEvaluatorMapper extends Mapper<LongWritable, Text, Text, TrafficWindow> {
    private static final Logger logger = LoggerFactory.getLogger(TrafficEvaluatorMapper.class);

    @Override
    protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text, Text,TrafficWindow>.Context context) throws java.io.IOException, java.lang.InterruptedException { /* compiled code */
        String line = value.toString();
        String[] parts = line.split("\t");
        if (parts.length<3) {
            logger.error("Line is too short: " + line);
            return;
        }

        String tmc = parts[0];

        context.write(new Text(tmc), new TrafficWindow(tmc, Long.parseLong(parts[1]), Integer.parseInt(parts[2])));
    }
}

class TrafficEvaluatorReducer extends Reducer<Text, TrafficWindow, Text, TrafficWindow> {
    private static final long TIME_WINDOW = 5*60*1000;

    protected void reduce(Text key, java.lang.Iterable<TrafficWindow> values,
                          org.apache.hadoop.mapreduce.Reducer<Text,TrafficWindow,Text,TrafficWindow>.Context context) throws java.io.IOException, java.lang.InterruptedException {
        Iterator<TrafficWindow> itr = values.iterator();
        long start = -1;
        long end = -1;
        int total =0;
        int count =0;

        while (itr.hasNext()) {
            TrafficWindow tw = itr.next();

            if (start==-1) {
                total =0;
                count =0;
                start = tw.getStartTime();
                end = start + TIME_WINDOW;
            }

            total += tw.getAvgSpeed();
            ++count;
            if (tw.getEndTime()>=end) {
                TrafficWindow redTW = new TrafficWindow(tw.getTmc(), start, tw.getEndTime(), total/(count-1));
                context.write(new Text(tw.getTmc()), redTW);
                start = -1;
            }
        }
    }
}



class TrafficWindow implements Writable {
    private String tmc;
    private int avgSpeed;
    private long startTime;
    private long endTime;

    public TrafficWindow() {
    }

    public TrafficWindow(String tmc, long startTime, int avgSpeed) {
        this(tmc, startTime,startTime,avgSpeed);
    }

    public TrafficWindow(String tmc, long startTime, long endTime, int avgSpeed) {
        this.startTime = startTime;
        this.avgSpeed = avgSpeed;
        this.endTime = endTime;
    }

    public int getAvgSpeed() {
        return avgSpeed;
    }

    public void setAvgSpeed(int avgSpeed) {
        this.avgSpeed = avgSpeed;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }



    public boolean equals(Object o) {
        if (o instanceof TrafficWindow) {
            TrafficWindow that = (TrafficWindow)o;
            if (this.avgSpeed==that.avgSpeed && this.startTime==that.startTime && this.endTime==that.endTime) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, tmc);
        WritableUtils.writeVLong(dataOutput, startTime);
        WritableUtils.writeVLong(dataOutput, endTime);
        WritableUtils.writeVInt(dataOutput, avgSpeed);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.tmc = WritableUtils.readString(dataInput);
        this.startTime = WritableUtils.readVLong(dataInput);
        this.endTime = WritableUtils.readVLong(dataInput);
        this.avgSpeed = WritableUtils.readVInt(dataInput);
    }

    public String getTmc() {
        return tmc;
    }

    public void setTmc(String tmc) {
        this.tmc = tmc;
    }
}