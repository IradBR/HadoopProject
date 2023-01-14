import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import static software.amazon.awssdk.profiles.ProfileProperty.AWS_ACCESS_KEY_ID;
import static software.amazon.awssdk.profiles.ProfileProperty.AWS_SECRET_ACCESS_KEY;

public class Step2 {

    private static final String SPACE = " ";
    private static final String TAB = "\t";
    private static final String BUCKETNAME = "bucketnamehadoop2project2";

    /**
     * Input:
     *      <key, value> -   <<r B> , <w1,w2,w3> >
     *                       <<r0 A> , <0, r1>>
     *                       <<r1 A> , <1, r0>>
     *
     * Output:
     *      <key, value> -   <<r B>, <w1,w2,w3> >
     *                       <<r0 A> , <0, r1 , 1>>
     *                       <<r1 A> , <1, r0 , 1>>
     */
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] record = value.toString().split(TAB);
                context.write(new Text(record[0]) , new Text(record[1] + SPACE + 1));
        }
    }


    /**
     * Input:
     *      <key, value> -   <<r B>, <w1,w2,w3> >
     *                       <<r0 A> , <0, r1 , 1>>
     *                       <<r1 A> , <1, r0 , 1>>
     *
     * Output:
     *      <key, value> -   <<r B>, <w1,w2,w3> >
     *                       <<r0 A> , <0, sum r1 , sum_occurrences_section>>
     *                       <<r1 A> , <1, sum r0 , sum_occurrences_section>>
     */
    public static class Combine extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long[] nr = new long[2];
            long[] tr = new long[2];

            String[] keyArray = key.toString().split(SPACE);
            if (keyArray[1].equals("A")) {
                for (Text val : values) {
                    String[] value = val.toString().split(SPACE);
                    int section = Integer.parseInt(value[0]);
                    nr[section]++;
                    tr[section] += Long.parseLong(value[1]);
                }

                if (nr[0] > 0)
                    context.write(key, new Text(0 + SPACE + tr[0] + SPACE + nr[0]));
                if (nr[1] > 0)
                    context.write(key, new Text(1 + SPACE + tr[1] + SPACE + nr[1]));
            } else { // B
                for (Text val : values) {
                    context.write(key, val); // <<r B>, <w1,w2,w3>>
                }
            }
        }
    }


    /**
     * Input:
     *      <key, value> -   <<r B>, <w1,w2,w3>>
     *                       <<r0 A> , <0, sum r1 , sum_occurrences_section>>
     *                       <<r1 A> , <1, sum r0 , sum_occurrences_section>>
     *
     * Output:
     *      <key, value> - < <w1,w2,w3> , <P(r)>> // for each 3gram
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private static double rProbability = -1;
        private static long rA = 0;
        private static long n = 47577291; //for hebrow curpos without stopwords

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long[] nr = new long[2];
            long[] tr = new long[2];
//            if (n == -1)
//                n = Long.parseLong(context.getConfiguration().get("N"));

            String[] keyArray = key.toString().split(SPACE);
            if (keyArray[1].equals("A")) {
                for (Text val : values) {
                    String[] value = val.toString().split(SPACE);
                    int section = Integer.parseInt(value[0]);
                    nr[section] += Long.parseLong(value[2]);
                    tr[section] += Long.parseLong(value[1]);
                }

                if (nr[0] != 0 || nr[1] != 0) {
                    rProbability = ((double) tr[0] + tr[1]) / (n * (nr[0] + nr[1]));
                } else {
                    rProbability = 0.0;
                }
                rA = Long.parseLong(keyArray[0]);
            } else { // B
                for (Text val : values) {
                    if (rA == Long.parseLong(keyArray[0])) {
                        context.write(val, new Text(String.valueOf(rProbability))); // <<w1 w2 w3> p(r) >
                    } else {
                        rProbability = 0.0;
                        context.write(val, new Text(String.valueOf(rProbability))); // <<w1 w2 w3> p(r) = 0.0 > - no enough data for calculation
                    }
                }
                rProbability = -1;
            }
        }
    }

    public static class Partition extends Partitioner<Text,Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] keyArray = key.toString().split(SPACE);
            return Math.abs(keyArray[0].hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        conf.set("N", "4730789"); //for our half english corpus
//        conf.set("N", "163471963"); //for hebrew corpus
//        conf.set("N", "23260642968"); //for english corpus
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step2.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Step2.Combine.class); //TODO to check with and without
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(Step2.Partition.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

        //for AWS run:
        String input="s3://" + BUCKETNAME + "/output1/";
        String output="s3://" + BUCKETNAME + "/output2";

//        //for local run:
//        String input = "/home/irad/IdeaProjects/aws_hadoop_2/output1";
//        String output = "output2";

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
