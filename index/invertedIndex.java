
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;  
import java.util.Iterator;  


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;



public class invertedIndex{

        public static class WordCountMapper extends Mapper<Object, Text, Text, Text>{
                private Text word = new Text();
                private FileSplit fsplit;
                
                @Override
                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        StringTokenizer itr = new StringTokenizer(value.toString());
                        fsplit = (FileSplit) context.getInputSplit();
                        //String[] temp = fsplit.getPath().toString().split("/");
                        //String pathInfo = temp[temp.length-1];
                        String pathInfo = itr.nextToken().toString();
			while (itr.hasMoreTokens()) {
                                word.set(itr.nextToken());
                                context.write(word, new Text(pathInfo));
                        }
                }
        }


        public static class WordCountReducer extends Reducer<Text,Text,Text,Text> {
                @Override
                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

                        HashMap<String, Integer> map=new HashMap<String, Integer>(); 
                        for (Text v : values) {
                                String val = v.toString();
                                if(map.containsKey(val)){
                                    int count = map.get(val);
                                    map.put(val, ++count);    
                                }
                                else{
                                    map.put(val, 1);
                                }
                        }
                        String valueList = "";
                        Iterator<String> itr = map.keySet().iterator();    
                        while (itr.hasNext()) {    
                                Object k = itr.next();    
                                valueList += k + ":" + map.get(k) + " ";
                        }
                        context.write(key, new Text(valueList));
                }
        }


        public static void main(String[] args) throws Exception {
                if (args.length != 2){
                        System.err.println("Usage: Word Count <input path> <output path>");
                }
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf, "invertedIndex");
                job.setJarByClass(invertedIndex.class);

                job.setMapperClass(WordCountMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                job.setReducerClass(WordCountReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                System.exit(job.waitForCompletion(true) ? 0 : 1);
                job.waitForCompletion(true);
        }
}








