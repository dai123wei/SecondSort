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
 
 
public class SecondarySortTest{
    /**
     * @ClassName IntPair
     * @Description 定义IntPair对象，该对象实现WritableComparable接口，描述第一列和第二列数据，同时完成两列数据的相关操作，这里是对二者进行比较
     * 
     */
    public static class IntPair implements WritableComparable<IntPair> {
        String first;
        int second;
 
        /**
         * Set the left and right values.
         */
        public void set(String left, int right) {
            first = left;
            second = right;
        }
 
        public String getFirst() {
            return first;
        }
 
        public int getSecond() {
            return second;
        }
 
        
        // 反序列化，从流中的二进制转换成IntPair
        public void readFields(DataInput in) throws IOException {
            // TODO Auto-generated method stub
            first = in.readUTF();
            second = in.readInt();
        }
 
        
        // 序列化，将IntPair转化成使用流传送的二进制
        public void write(DataOutput out) throws IOException {
            // TODO Auto-generated method stub
            out.writeUTF(first);
            out.writeInt(second);
        }
 
        
        // key的比较
        public int compareTo(IntPair o) {
        	int minus = this.getFirst().compareTo(o.getFirst());
			if (minus != 0){
				return minus;
			}
			return this.second - o.second;
        }
 
        @Override
        public boolean equals(Object right) {   
        	if (right == null)
                return false;
            if (this == right)
                return true;
            if (right instanceof IntPair) {
                IntPair r = (IntPair) right;
                return r.first.equals(first) && r.second == second;
            } else {
                return false;
            }
        }
    }
 
    // 第二种方法，继承WritableComparator
    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(IntPair.class, true);
            System.out.println("GroupingComparator---------------------------------");
        }
 
        @Override
        // Compare two WritableComparables.
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            return ip1.getFirst().compareTo(ip2.getFirst());
        }
    }
 
    /**
     * @ClassName Map
     * @Description 自定义map类，将每行数据进行分拆，第一列的数据存入left变量，第二列数据存入right变量
     *              在map阶段的最后，会先调用job.setPartitionerClass对这个List进行分区，每个分区映射到一个reducer
     *              。每个分区内又调用job.setSortComparatorClass设置的key比较函数类排序。可以看到，这本身就是一个二次排序。
     */
    public static class Map extends Mapper<LongWritable, Text, IntPair, IntWritable> {
        private final IntPair intkey = new IntPair();
        private final IntWritable intvalue = new IntWritable();
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            // 调用java自己的工具类StringTokenizer(),将map输入的每行字符串按规则进行分割成每个字符串，这些规则有\t\n\r\f，基本上分割的结果都可以保证到最细的字符串粒度
            StringTokenizer tokenizer = new StringTokenizer(line);
            String left = "";
            int right = 0;
            if (tokenizer.hasMoreTokens()) {
                left = tokenizer.nextToken();
                System.out.println("left: " + left);
                if (tokenizer.hasMoreTokens())
                    right = Integer.parseInt(tokenizer.nextToken());
                intkey.set(left, right);
                intvalue.set(right);
                context.write(intkey, intvalue);
            }
        }
    }
 
    // 自定义reduce
    public static class Reduce extends Reducer<IntPair, IntWritable, Text, Text> {
    	StringBuffer sb = new StringBuffer();
    	Text score = new Text();
        private final Text left = new Text();
        private static final Text SEPARATOR = new Text("------------------------------------------------");
        public void reduce(IntPair key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
        	left.set(key.getFirst());
        	//先清除上一个组的数据
    		sb.delete(0, sb.length());
    		
    		for (IntWritable val : values){
    			sb.append(val.get() + ",");
    		}
    		
    		//取出最后一个逗号
    		if (sb.length() > 0){
    			sb.deleteCharAt(sb.length() - 1);
    		}
    		
    		//设置写出去的value
    		score.set(sb.toString());
    		
    		//将联合Key的第一个元素作为新的key，将score作为value写出去
    		context.write(left, score);
        }
    
    }
 
    /**
     * @param args
     */
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
 
        Configuration conf = new Configuration(true);
        String[] otherArgs = new String[2];
        otherArgs[0] = "hdfs://localhost:9000/user/dw/input/sort.txt";
        String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        otherArgs[1] = "hdfs://localhost:9000/user/dw/mr-" + time;
 
        Job job = new Job(conf, "secondarysorttest");
        job.setJarByClass(SecondarySort.class);
        job.setMapperClass(Map.class);
        
        // 分组函数
        job.setGroupingComparatorClass(GroupingComparator.class);
 
        // Reducer类型
        job.setReducerClass(Reduce.class);
 
        // map输出Key的类型
        job.setMapOutputKeyClass(IntPair.class);
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
