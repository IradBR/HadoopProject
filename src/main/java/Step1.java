import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class Step1 {


    private static final String SPACE = " ";
    private static final String TAB = "\t";
    private static final String BUCKETNAME = "bucketnamehadoop2project2";
    private static final String stopWordsString = "״\n" +
            "׳\n" +
            "של\n" +
            "רב\n" +
            "פי\n" +
            "עם\n" +
            "עליו\n" +
            "עליהם\n" +
            "על\n" +
            "עד\n" +
            "מן\n" +
            "מכל\n" +
            "מי\n" +
            "מהם\n" +
            "מה\n" +
            "מ\n" +
            "למה\n" +
            "לכל\n" +
            "לי\n" +
            "לו\n" +
            "להיות\n" +
            "לה\n" +
            "לא\n" +
            "כן\n" +
            "כמה\n" +
            "כלי\n" +
            "כל\n" +
            "כי\n" +
            "יש\n" +
            "ימים\n" +
            "יותר\n" +
            "יד\n" +
            "י\n" +
            "זה\n" +
            "ז\n" +
            "ועל\n" +
            "ומי\n" +
            "ולא\n" +
            "וכן\n" +
            "וכל\n" +
            "והיא\n" +
            "והוא\n" +
            "ואם\n" +
            "ו\n" +
            "הרבה\n" +
            "הנה\n" +
            "היו\n" +
            "היה\n" +
            "היא\n" +
            "הזה\n" +
            "הוא\n" +
            "דבר\n" +
            "ד\n" +
            "ג\n" +
            "בני\n" +
            "בכל\n" +
            "בו\n" +
            "בה\n" +
            "בא\n" +
            "את\n" +
            "אשר\n" +
            "אם\n" +
            "אלה\n" +
            "אל\n" +
            "אך\n" +
            "איש\n" +
            "אין\n" +
            "אחת\n" +
            "אחר\n" +
            "אחד\n" +
            "אז\n" +
            "אותו\n" +
            "־\n" +
            "^\n" +
            "?\n" +
            ";\n" +
            ":\n" +
            "1\n" +
            ".\n" +
            "-\n" +
            "*\n" +
            "\"\n" +
            "!\n" +
            "שלשה\n" +
            "בעל\n" +
            "פני\n" +
            ")\n" +
            "גדול\n" +
            "שם\n" +
            "עלי\n" +
            "עולם\n" +
            "מקום\n" +
            "לעולם\n" +
            "לנו\n" +
            "להם\n" +
            "ישראל\n" +
            "יודע\n" +
            "זאת\n" +
            "השמים\n" +
            "הזאת\n" +
            "הדברים\n" +
            "הדבר\n" +
            "הבית\n" +
            "האמת\n" +
            "דברי\n" +
            "במקום\n" +
            "בהם\n" +
            "אמרו\n" +
            "אינם\n" +
            "אחרי\n" +
            "אותם\n" +
            "אדם\n" +
            "(\n" +
            "חלק\n" +
            "שני\n" +
            "שכל\n" +
            "שאר\n" +
            "ש\n" +
            "ר\n" +
            "פעמים\n" +
            "נעשה\n" +
            "ן\n" +
            "ממנו\n" +
            "מלא\n" +
            "מזה\n" +
            "ם\n" +
            "לפי\n" +
            "ל\n" +
            "כמו\n" +
            "כבר\n" +
            "כ\n" +
            "זו\n" +
            "ומה\n" +
            "ולכל\n" +
            "ובין\n" +
            "ואין\n" +
            "הן\n" +
            "היתה\n" +
            "הא\n" +
            "ה\n" +
            "בל\n" +
            "בין\n" +
            "בזה\n" +
            "ב\n" +
            "אף\n" +
            "אי\n" +
            "אותה\n" +
            "או\n" +
            "אבל\n" +
            "א\n";

    private static HashSet<String> stopWords = createHashSetStopWords();

    private static HashSet<String> createHashSetStopWords(){
        HashSet<String> newHash = new HashSet<>();
        String[] stopWordsArray = stopWordsString.split("\n");
        for (String word: stopWordsArray) {
            newHash.add(word);
        }
        return newHash;
    }

    /**
     * Input:
     *      <key, value> - <lineId , n-gram /t year /t occurrences /t pages /t books >
     * Output:
     *      <key, value> -   <<w1, w2, w3>, <occurrences, section>>
     */

    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(TAB);
            String[] trigram = fields[0].split(SPACE);
            if (trigram.length > 2 && fields.length > 2) {
                String w1 = trigram[0];
                String w2 = trigram[1];
                String w3 = trigram[2];
                String occurrences = fields[2];
                int section = (int)Math.round(Math.random());

                if(!isContainSpacialWord(w1, w2, w3)) {
//                    context.write(new Text("N"), new Text(occurrences));
                    String stringSection = String.valueOf(section);
                    context.write(new Text(w1 + SPACE + w2 + SPACE + w3), new Text(String.format("%s %s", occurrences, stringSection)));
                }
            }
        }

        private boolean isContainSpacialWord(String w1, String w2, String w3) throws FileNotFoundException {
            return stopWords.contains(w1) || stopWords.contains(w2) || stopWords.contains(w3);
        }
    }

    /**
     * Input:
     *      <key, value> -   <<w1, w2, w3>, <occurrences, section>>
     *
     * Output:
     *      <key, value> -   <<w1, w2, w3>, <sum_occurrences, section>>
     */
    public static class Combine extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           // long totalOccurrencesN = 0;
            long[] r = new long[2];
            //int KeyLength = key.toString().split(SPACE).length;
            long trigramOccurrences;
            int section;
            for (Text value : values) {
//                if (KeyLength == 3) { // trigram
                    String[] valueArray = value.toString().split(SPACE);
                    trigramOccurrences = Long.parseLong(valueArray[0]);
                    section = Integer.parseInt(valueArray[1]);
                    r[section] += trigramOccurrences;
//                } else { // N
//                    totalOccurrencesN += Long.parseLong(value.toString());
//                }
            }
//            if (KeyLength == 3) { // Trigram
                writeKeyValueForTrigramToContent(key, context, r);
//            } else { // N
//                context.write(new Text("N"), new Text(String.valueOf(totalOccurrencesN)));
//            }
        }

        private void writeKeyValueForTrigramToContent(Text key, Reducer<Text, Text, Text, Text>.Context context, long[] r) throws IOException, InterruptedException {
            if (r[0] > 0)
                context.write(key , new Text(r[0] + SPACE + 0));
            if (r[1] > 0)
                context.write(key , new Text(r[1] + SPACE + 1));
        }
    }

    /**
     * Input:
     *      <key, value> -   <<w1, w2, w3>, <sum_occurrences, section>>
     *
     * Output:
     *      <key, value> -   <<r B> , <w1,w2,w3>>
     *                       <<r0 A> , 0, r1>
     *                       <<r1 A> , 1, r0>
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            long totalOccurrencesN = 0;
            long[] r = new long[2];
//            int KeyLength = key.toString().split(SPACE).length;
            long trigramOccurrences;
            int section;
            for (Text value : values) {
//                if (KeyLength == 3) { // trigram
                    String[] valueArray = value.toString().split(SPACE);
                    trigramOccurrences = Long.parseLong(valueArray[0]);
                    section = Integer.parseInt(valueArray[1]);
                    r[section] += trigramOccurrences;
//                } else { // N
//                    totalOccurrencesN += Long.parseLong(value.toString());
//                }
            }
//            if(KeyLength == 3){ // Trigram
                writeKeyValueForTrigramToContent(key, context, r);
//            }else{ // N
//                context.write(new Text("N"), new Text(String.valueOf(totalOccurrencesN)));
//            }
        }

        private void writeKeyValueForTrigramToContent(Text key, Reducer<Text, Text, Text, Text>.Context context, long[] r) throws IOException, InterruptedException {
            long totalTrigramOccurrencesR = r[0] + r[1];
            context.write(new Text(String.valueOf(totalTrigramOccurrencesR) + SPACE + "B"), key);
            if(r[0] > 0)
                context.write(new Text(r[0] + SPACE + "A"), new Text(0 + SPACE + r[1]));
            if(r[1] > 0)
                context.write(new Text(r[1] + SPACE + "A"), new Text(1 + SPACE + r[0]));
        }
    }

    public static class Partition extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step1.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class); //TODO to check with and without
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(Step1.Partition.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // for AWS run:
        //google:
        job.setInputFormatClass(SequenceFileInputFormat.class); //for ngram google
        String input =  "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
        String output = "s3://" + BUCKETNAME + "/output1";
        SequenceFileInputFormat.addInputPath(job, new Path(input)); //for ngram google
        //text:
//        job.setInputFormatClass(TextInputFormat.class);
//        String input = "s3://" + BUCKETNAME + "/3_grams.txt";
//        String output = "s3://" + BUCKETNAME + "/output1";
//        FileInputFormat.addInputPath(job, new Path(input));


        //for local run:
//        job.setInputFormatClass(TextInputFormat.class);
//        String input = "/home/irad/IdeaProjects/aws_hadoop_2/3_grams.txt";
//        String output = "output1";
//        FileInputFormat.addInputPath(job, new Path(input));

        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
