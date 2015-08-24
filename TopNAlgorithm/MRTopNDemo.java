package com.wxp.topN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** A map-reduce max-heap based implementation for Top N algorithm
    HOW TO RUN: hadoop jar jarfile com.wxp.topN.MRTopNDemo -D N=5 input output
	Example input file:

		1 324 5 6 2, 2 	5
        3434 430
		a
		34, 	590 
 
*/
  
public class MRTopNDemo extends Configured implements Tool {
	

	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new Configuration(), new TopNDemo(), args);
		System.out.println(exitcode);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		if (arg0.length != 2){
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		// get Job instance 
		Job job = Job.getInstance(this.getConf());
		// set Jar
		job.setJarByClass(getClass());
		// set input output path
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		return job.waitForCompletion(true)? 0 : 1;
	}

	public static class TopNMapper extends 
			Mapper<LongWritable, Text, NullWritable, Text>{
		
		// use max-heap to maintain the top N element
		private PriorityQueue<Integer> maxheap;
		private int N;
		private Text holder = new Text();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String n_string = conf.get("N");
			this.N = Integer.parseInt(n_string);
			this.maxheap = new PriorityQueue<>(this.N, Collections.reverseOrder());
		}
		
		public void map(LongWritable key, Text value, Context context)throws 
			IOException, InterruptedException{
				// clean the input file
				String[] numbers = value.toString().split("[\\s,;\\n\\t]+");
				for (String num : numbers){
					// return Integer.min if parse failed
					Integer numeric = NumberUtils.toInt(num, Integer.MIN_VALUE);
					insertIntoHeap(numeric, maxheap, N);
				}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			while (!maxheap.isEmpty()){
				Integer elem = maxheap.remove();
				holder.set(elem.toString());
				context.write(NullWritable.get(), holder);
			}
		}
	
	}
	
	public static class TopNReducer 
			extends Reducer<NullWritable, Text, NullWritable, Text>{
		
		private PriorityQueue<Integer> maxheap;
		private int N;
		private Text holder = new Text();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			String n_string = conf.get("N");
			this.N = Integer.parseInt(n_string);
			maxheap = new PriorityQueue<>(this.N, Collections.reverseOrder());
		}
		
		
		public void reduce(NullWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			for (Text value: values){
				Integer num = NumberUtils.toInt(value.toString());
				insertIntoHeap(num, maxheap, N);
			}
			
			List<Integer> list = new ArrayList<>();
			
			while (!maxheap.isEmpty()){
				list.add(maxheap.remove());
			}
			// sort the N result;
			Collections.reverse(list);
			
			for (Integer num : list){
				holder.set(num.toString());
				context.write(NullWritable.get(), holder);
			}
		}
	}
	
	private static void insertIntoHeap(Integer num, 
			PriorityQueue<Integer> heap, int sizeLimit){
		if (num.equals(Integer.MIN_VALUE)){
			return;
		}
		if(heap.size() < sizeLimit){
			heap.add(num);
		}else{
			int top = heap.peek();
			if(num < top){
				heap.remove();
				heap.add(num);
			}
		}
	}
}
