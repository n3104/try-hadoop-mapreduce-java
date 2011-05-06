package jp.gr.java_conf.n3104.try_mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Employee.txt を年齢順にソートします。
 * 
 * @author n3104
 */
public class SortByAgeUsingHashPartitioner extends Configured implements Tool {

	public static class SortByAgeMapper extends MapReduceBase implements
			Mapper<Object, Text, IntWritable, Text> {

		@Override
		public void map(Object key, Text value, OutputCollector<IntWritable, Text> output,
				Reporter reporter) throws IOException {
			String[] values = value.toString().split(",");
			int age = Integer.parseInt(values[2]);
			output.collect(new IntWritable(age), value);
		}
	}

	public static class SortByAgeReducer extends MapReduceBase implements
			Reducer<IntWritable, Text, IntWritable, Text> {

		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			while (values.hasNext()) {
				output.collect(key, values.next());
			}
		}
	}

	public static class DescendingKeyComparator extends WritableComparator {

		protected DescendingKeyComparator() {
			super(IntWritable.class, true);
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return a.compareTo(b) * -1;
		}
	}

	public static class DescendingKeyRawComparator extends IntWritable.Comparator {
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return super.compare(b1, s1, l1, b2, s2, l2) * -1;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setMapperClass(SortByAgeMapper.class);
		// conf.setOutputKeyComparatorClass(DescendingKeyComparator.class);
		conf.setReducerClass(SortByAgeReducer.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		// HashPartitionerを利用
		conf.setPartitionerClass(HashPartitioner.class);
		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// 引数を固定で設定
		String in = SortByAgeUsingHashPartitioner.class.getResource("Employee.txt").getPath();
		String out = Util.getJobOutputDirPath(SortByAgeUsingHashPartitioner.class);
		args = new String[] { in, out };
		// 出力先のディレクトリが存在するとFileAlreadyExistsExceptionとなるため事前に削除しています
		FileUtil.fullyDelete(new File(out));

		int res = ToolRunner.run(new SortByAgeUsingHashPartitioner(), args);
		System.exit(res);
	}

}
