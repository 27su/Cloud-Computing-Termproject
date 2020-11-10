package mju.hadoop.termproject;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TermProject {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static IntWritable price = new IntWritable(0);
        
        private Text word = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        			int p, index = 0;
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line,",\n");
            while(tokenizer.hasMoreTokens()) {
            	String str = tokenizer.nextToken();
            	
            	if(index == 2){ //품목ID
            					word.set(str); 
            						}
            	else if(index == 6){ //정가보다 할인된 가격 = 할인가격
            					p = Integer.parseInt(str);
            					price.set(p);
            					context.write(word, price);
            						}
            	
            	if(index == 7){
            		index=0;
            						}
            	else 
            		index++;
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable value = new IntWritable(0);

					@Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
								Configuration configuration = context.getConfiguration();		
								String pID = configuration.get("option");
        	
        	   int min = Integer.MAX_VALUE;
        	   int max = Integer.MIN_VALUE;
        	   long avg = 0, count=0, sum=0;
        	   int dif = 0;
        	   
            for (IntWritable value : values){
            	min = Math.min(min, value.get()); //최소할인가격
            	max = Math.max(max, value.get()); //최대할인가격  
            	sum += value.get();
            	count++;
            					}
            
            avg = sum/count;//평균할인가격  
            dif = max - min;
            	
            if(pID.equals("-1")) {//디폴트일때는 품목별 할인가격의 물가변동 출력  
            				value.set(dif);
            				context.write(key, value);
            					} 
            else if(pID.equals("average")){ //average라는 옵션이 입력값으로 들어오면 품목별 평균 할인 가격 출력  
            	value.set((int)avg);
            	context.write(key, value);
            					}
            else if(pID.equals(key.toString())){ //품목ID가 입력값으로 들어오면 해당품목의 할인가격 물가변동  출력  
            	value.set(dif);
            	context.write(key, value);
            					}
        }
    }

    public static void main(String[] args) throws Exception {
    	    
        Configuration conf = new Configuration();
        if(args.length == 3) conf.set("option",args[2]); 
        else conf.set("option", "-1"); //입력값이 없으면 -1로 옵션값 설정  
        Job job = new Job(conf, "termproject");
        job.setJarByClass(TermProject.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);
        

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
       
        boolean success = job.waitForCompletion(true);
        System.out.println(success);
                     
                 
    }
}