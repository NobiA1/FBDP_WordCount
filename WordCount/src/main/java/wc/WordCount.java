package wc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.System;
import java.util.regex.Pattern;
import java.net.URI;
//import java.util.Comparator;
//import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.TreeMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* The first job with WordCountMapper.class and WordCountReducer.class aims to count the frequency of different words in TEXT(csv)
 * We should replace all nums ,stopwords and punctuation marks(such as '.','/''?'...)
 * The second job with SortMapper.class and SortReducer.class aims to reorder the record with decreasing value order (and normal order with same value)
 * With TopN(default:100)
 */
public class WordCount {
    
    public static class WordCountMapper extends Mapper<Object,Text,Text,IntWritable>{
		private Set<String> stopwords;
		private String swlpath;

		@Override
		public void setup(Context context) throws IOException,InterruptedException{

			stopwords = new TreeSet<String>();
			Configuration conf = context.getConfiguration();
			swlpath  = conf.get("stopwords");//stopword-list-path
			FileSystem fs = FileSystem.get(URI.create(swlpath), conf);  
			FSDataInputStream InputStream = fs.open(new Path(swlpath));  
			InputStreamReader isr = new InputStreamReader(InputStream, "utf-8");  
			String line;//read stop-word-list.txt line by line
			BufferedReader br = new BufferedReader(isr);
			while ((line = br.readLine()) != null) {
				StringTokenizer itr = new StringTokenizer(line);
				while (itr.hasMoreTokens()) {
					stopwords.add(itr.nextToken());
				}
			}
		}
		//allnums and punctuation marks
		Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$"); 
		@Override
		public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
			
			String temp = new String();
			final IntWritable one = new IntWritable(1);
			Text word = new Text();
			StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().replaceAll("\\pP|\\pS", " "));
			//StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().replaceAll("\\pP|\\pS", ""));
			for(;itr.hasMoreTokens();){
				temp = itr.nextToken();
				// 如果是数字则不保存
				if (pattern.matcher(temp).matches()){
					continue;
				}
				if (!stopwords.contains(temp)) {
					word.set(temp);
					context.write(word, one);
				}
			}
		}
	}
	
	public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

		private IntWritable result = new IntWritable();
		@Override
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key,result);
		}
	}

	public static class SortMapper extends Mapper<Object, Text, Text, IntWritable>{
        private int topN=100;  // the TopN(default:100)
        //private TreeMap<Integer, String> word_list=new TreeMap<Integer, String>(); // local list with words sorted by their frequency
		private TreeMap<Float,String> word_list = new TreeMap<Float,String>((o1,o2)->o2.compareTo(o1));
		private Random random =new Random(0);
        public void setup(Context context){
            topN = Integer.parseInt(context.getConfiguration().get("topN"));  // get N
        }

        public void map(Object key, Text value, Context context)
        {
            String[] line = value.toString().split("\t");   // split the word and the wordcount
            // put the wordcount as key and the word as value in the word list
            // so the words can be sorted by their wordcounts
            word_list.put(Integer.valueOf(line[1])+random.nextFloat(),line[0]);
            // if the local word list is populated with more than N elements
            // remove the first (aka remove the word with the smallest wordcount)
            if (word_list.size() > topN)
                word_list.remove(word_list.lastKey());
        }

        public void cleanup(Context context) throws IOException, InterruptedException
        {
            // write the topN local words before continuing to SortReducer
            // with each word as key and its wordcount as value
            for (Map.Entry<Float, String> entry : word_list.entrySet())
            {
                String word = entry.getValue();
                Integer wordcount = (int) Math.floor(entry.getKey());
                context.write(new Text(word), new IntWritable(wordcount));
            }
        }
    }

	public static class SortReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private int topN=100;  
        private TreeMap<Float, String> word_list=new TreeMap<Float, String>((o1,o2)->o2.compareTo(o1));
		private Random random =new Random(0);

        public void setup(Context context){
            topN = Integer.parseInt(context.getConfiguration().get("topN")); 
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context){
            int wordcount = 0;

            for(IntWritable value : values)
                wordcount = value.get();

            word_list.put(wordcount+random.nextFloat(), key.toString());

            if (word_list.size() > topN)
                word_list.remove(word_list.lastKey());
        }

        public void cleanup(Context context) throws IOException, InterruptedException{
            // write the topN global words with each word as key and its wordcount as value
			int count=0;
            for (Map.Entry<Float, String> entry : word_list.entrySet()){
                String word = entry.getValue();
                Integer wordcount = (int) Math.floor(entry.getKey());
                context.write(new Text(String.valueOf(count+1)+":"+word), new IntWritable(wordcount));
				count++;
            }
        }
    }

    
    
    public static void main(String[] args ) throws Exception{
        if(args.length!=4){
			System.out.println("Incorrect Parameters Numbers!\nUsage: wordcount <input> <stoplist> <output> <length>\n");
		}
        Configuration wcconf = new Configuration(true);
		//stconf.set("mapreduce.textoutputformat.separator", ",");//define output form as key,value
        wcconf.set("stopwords", args[1]);
		//stconf.set("topN", args[3]);
        Job WC_job = Job.getInstance(wcconf,"Word Count");
        WC_job.setJarByClass(WordCount.class);
		WC_job.setMapperClass(WordCountMapper.class);
		WC_job.setCombinerClass(WordCountReducer.class);
		WC_job.setReducerClass(WordCountReducer.class);
        WC_job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(WC_job,new Path(args[0]));
		WC_job.setOutputKeyClass(Text.class);
		WC_job.setOutputValueClass(IntWritable.class);
        //job.setSortComparatorClass(Sort.class);
		FileOutputFormat.setOutputPath(WC_job,new Path("tmp"));//restore temp output
		WC_job.waitForCompletion(true);
		System.out.println("Job WordCount has been successfully done!\n");

		Configuration stconf = new Configuration(true);
		stconf.set("mapreduce.output.textoutputformat.separator", ",");//define output form as key,value
		stconf.set("topN", args[3]);
		Job Sort_job = Job.getInstance(stconf,"Sort TopN");
		Sort_job.setJarByClass(WordCount.class);
		Sort_job.setMapperClass(SortMapper.class);
		Sort_job.setReducerClass(SortReducer.class);
		Sort_job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(Sort_job, new Path("tmp"));
		Sort_job.setOutputKeyClass(Text.class);
		Sort_job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(Sort_job, new Path(args[2]));
		Sort_job.waitForCompletion(true);
		System.out.println("Job Sort and TopN has been successfully done!\n");
        System.exit(0);
    }
}
