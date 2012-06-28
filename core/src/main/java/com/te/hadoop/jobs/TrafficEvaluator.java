package com.te.hadoop.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.math.Ordering;

import javax.xml.crypto.dsig.keyinfo.KeyInfo;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


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
    public int run(java.lang.String[] args) throws Exception {
        Configuration conf =this.getConf();
        conf.setQuietMode(false);

        Job job = new Job(conf, "Traffic Eye");

        job.setJarByClass(this.getClass());

        job.setMapperClass(TrafficEvaluatorMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TrafficWindow.class);
        job.setReducerClass(TrafficEvaluatorReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TrafficWindow.class);

        for (int i= 0; i<args.length - 1; ++i) {
            System.err.println(String.format("processing %d: %s", i, args[i]));
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TrafficEvaluator(), args);
        System.err.println("The args are: " + Arrays.asList(args));
        System.exit(res);
//        String dt = "2011-04-05 19:00:00.000";
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//        System.out.println("date: " + simpleDateFormat.parse(dt));

    }
}

class TrafficEvaluatorMapper extends Mapper<LongWritable, Text, Text, TrafficWindow> {
    private static final Logger logger = LoggerFactory.getLogger(TrafficEvaluatorMapper.class);
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable,Text, Text,TrafficWindow>.Context context) throws java.io.IOException, java.lang.InterruptedException { /* compiled code */
        String line = value.toString();
        String[] parts = line.split("\t");
        if (parts.length<3) {
            logger.error("Line is too short: " + line);
            return;
        }

        String tmc = parts[0];
        String st = parts[1];

        try {
            TrafficWindow tw = new TrafficWindow(tmc, simpleDateFormat.parse(st).getTime(), Integer.parseInt(parts[2]));
            System.err.println("Map -> writing out " + tw);
            context.write(new Text(tmc), tw);
        } catch (ParseException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}

class TrafficEvaluatorReducer extends Reducer<Text, TrafficWindow, Text, TrafficWindow> {
    private static final long TIME_WINDOW = 5*60*1000;

    protected void reduce(Text key, java.lang.Iterable<TrafficWindow> values,
                          org.apache.hadoop.mapreduce.Reducer<Text,TrafficWindow,Text,TrafficWindow>.Context context) throws java.io.IOException, java.lang.InterruptedException {
        System.err.println("Processing tmc: " + key);

        Iterator<TrafficWindow> itr = values.iterator();
        long start = -1;
        long end = -1;
        int total =0;
        int count =0;

        while (itr.hasNext()) {
            TrafficWindow tw = itr.next();
            System.err.println("next entry " + tw);

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
                System.err.println("Reduce -> writing out: " + redTW);
                context.write(new Text(tw.getTmc()+":"+tw.getEndTime()), redTW);
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
        this.tmc = tmc;
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
            if (this.tmc.equals(that.tmc) && this.avgSpeed==that.avgSpeed && this.startTime==that.startTime && this.endTime==that.endTime) {
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

    @Override
    public String toString() {
        return this.tmc+":"+this.startTime+":"+this.endTime+":"+this.avgSpeed;
    }
}