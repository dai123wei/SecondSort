package com.dw.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.dw.sort.SecondarySort.GroupingComparator;
import com.dw.sort.SecondarySort.IntPair;
import com.dw.sort.SecondarySort.Map;
import com.dw.sort.SecondarySort.Reduce;

public class SecondarySortTemp {

	public static class DataPair implements WritableComparable<DataPair>{
		int year;
		int month;
		int temp;
		public int getYear() {
			return year;
		}
		public int getMonth() {
			return month;
		}
		public int getTemp() {
			return temp;
		}
		public void set(int year, int month, int temp) {
			this.year = year;
			this.month = month;
			this.temp = temp;
		}
		
		public void readFields(DataInput in) throws IOException {
			year = in.readInt();
			month = in.readInt();
			temp = in.readInt();
		}
		
		public void write(DataOutput out) throws IOException {
			out.writeInt(year);
			out.writeInt(month);
			out.writeInt(temp);
		}
		
		public int compareTo(DataPair o) {
			if(year != o.year) {
				return year < o.year ? -1 : 1;
			}else if(month != o.month) {
				return month < o.month ? -1 : 1;
			}else if(temp != o.temp) {
				return temp < o.temp ? -1 : 1;
			}else {
				return 0;
			}
		}
		
		public boolean equals(Object other) {
			if(other == null) 
				return false;
			if(this == other)
				return true;
			if(other instanceof DataPair) {
				DataPair o = (DataPair) other;
				return o.year == year && o.month == month && o.temp == temp;
			}else
				return false;
		}
		
	}
	
	public static class GroupingComparator extends WritableComparator{
		protected GroupingComparator() {
			super(DataPair.class, true);
			System.out.println("GroupingComparator---------------------------------");
		}
		
		public int compare(WritableComparable w1, WritableComparable w2) {
			DataPair dp1 = (DataPair) w1;
			DataPair dp2 = (DataPair) w2;
			return dp1.year == dp2.year ? (dp1.month == dp2.month ? 0 : (dp1.month < dp2.month ? -1 : 1)) : (dp1.year < dp2.year ? -1 : 1);
		}
	}
	
	public static class TempMap extends Mapper<LongWritable, Text, DataPair, IntWritable>{
		private final DataPair datakey = new DataPair();
		private final IntWritable intvalue = new IntWritable();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] datas = line.split("\t");
			int year = Integer.parseInt(datas[0]);
			String[] strs = datas[1].split(",");
			int month = Integer.parseInt(strs[0]);
			int temp = Integer.parseInt(datas[2]);
			datakey.set(year, month, temp);
			intvalue.set(temp);
			context.write(datakey, intvalue);
		}
	}
	
	public static class TempReduce extends Reducer<DataPair, IntWritable, Text, Text>{
		StringBuffer sb = new StringBuffer();
		Text temp = new Text();
		private final Text date = new Text();
		private static final Text SEPARATOR = new Text("------------------------------------------------");
		public void reduce(DataPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			date.set(Integer.toString(key.getYear()) + "-" + Integer.toString(key.getMonth()));
			sb.delete(0,  sb.length());
			for(IntWritable val : values) {
				sb.append(val.get() + ",");
			}
			if(sb.length() > 0) {
				sb.deleteCharAt(sb.length()-1);
			}
			temp.set(sb.toString());
			
			context.write(date, temp);
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration(true);
        String[] otherArgs = new String[2];
        otherArgs[0] = "hdfs://localhost:9000/user/dw/input/temper.txt";
        String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        otherArgs[1] = "hdfs://localhost:9000/user/dw/mr-" + time;
 
        Job job = new Job(conf, "secondarysorttemp");
        job.setJarByClass(SecondarySortTemp.class);
        job.setMapperClass(TempMap.class);
        // 不再需要Combiner类型，因为Combiner的输出类型<Text,
        // IntWritable>对Reduce的输入类型<IntPair, IntWritable>不适用
        // job.setCombinerClass(Reduce.class);
 
        // 分区函数
        //job.setPartitionerClass(FirstPartitioner.class);
        // 分组函数
        job.setGroupingComparatorClass(GroupingComparator.class);
 
        // Reducer类型
        job.setReducerClass(TempReduce.class);
 
        // map输出Key的类型
        job.setMapOutputKeyClass(DataPair.class);
        // map输出Value的类型
        job.setMapOutputValueClass(IntWritable.class);
        // reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
        job.setOutputKeyClass(Text.class);
        // reduce输出Value的类型
        job.setOutputValueClass(Text.class);
 
        // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。
        job.setInputFormatClass(TextInputFormat.class);
        // 提供一个RecordWriter的实现，负责数据输出。
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
 
        // 提交job
        if (job.waitForCompletion(false)) {
            System.out.println("job ok !");
        } else {
            System.out.println("job error !");
        }
	}
	
}
