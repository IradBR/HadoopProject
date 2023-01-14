import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;

public class Step3 {

    /**
     * The Input:
     *      <key, value> - <<w1,w2,w3>, <p(r)>>
     *
     * The Output:
     *      <key, value> - <<w1,w2,p(r)>, <w3>>
     *
     */

    private static final String SPACE = " ";
    private static final String BUCKETNAME = "bucketnamehadoop2project2";
    private static final String TAB = "\t";

    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] keyArray = value.toString().split(TAB)[0].split(SPACE);
            String w1 = keyArray[0];
            String w2 = keyArray[1];
            String w3 = keyArray[2];
            String rProbability = value.toString().split(TAB)[1];
            context.write(new Text(w1 + SPACE + w2 + SPACE + rProbability) , new Text(w3));
        }

    }

    /**
     * The Input:
     *      <key, value> - <<w1,w2,p(r)>, <w3>>
     *
     * The Output:
     *      <key, value> - <<w1,w2,w3> , <P(r)>>
     */

    private static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keyArray = key.toString().split(SPACE);
            String w1 = keyArray[0];
            String w2 = keyArray[1];
            String rProbability = keyArray[2];

            for (Text value : values) {
                String w3 = value.toString();
                context.write(new Text(w1 + SPACE + w2 + SPACE + w3), new Text(rProbability));
            }
        }

    }

    public static class Partition extends Partitioner<Text,Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }

    private static class Comparison extends WritableComparator {
        protected Comparison() {
            super(Text.class, true);
        }
        @Override
        public int compare(WritableComparable key1, WritableComparable key2) { // keys: w1 w2 P(r)
            String[] word1Array = key1.toString().split(SPACE);
            String[] word2Array = key2.toString().split(SPACE);
            double rProbability1 = Double.parseDouble(word1Array[2]);
            double rProbability2 = Double.parseDouble(word2Array[2]);

            int diffWord1 = word1Array[0].compareTo(word2Array[0]);
            int diffWord2 = word1Array[1].compareTo(word2Array[1]);

            if(diffWord1 == 0){ // w1 = w1
                if(diffWord2 == 0){ // w2 = w2
                    if(rProbability1 - rProbability2 > 0 )
                        return -1;
                    else if(rProbability1 - rProbability2 < 0)
                        return 1;
                    else
                        return 0;
                  // the bigger probability => return val < 0
                }else{
                    return diffWord2;
                }
            }else{
                return diffWord1;
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setSortComparatorClass(Step3.Comparison.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Step3.Partition.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //for AWS run:
        String input="s3://" + BUCKETNAME + "/output2";
        String output="s3://" + BUCKETNAME + "/output3";
        //for local run:
//        String input = "/home/irad/IdeaProjects/aws_hadoop_2/output2";
//        String output = "output3";
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}