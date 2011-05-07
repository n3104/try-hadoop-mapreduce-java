package jp.gr.java_conf.n3104.try_mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.SecondarySort.FirstGroupingComparator;
import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 従業員ファイルを部門と年齢でソートします。
 * <p>
 * Mapper のキーを {@code demartmentId} と {@code employeeAge} の複合キーにして、
 * シャッフル時の Secondary Sort を利用してソートしています。
 * </p>
 * 
 * @author n3104
 */
public class SortByDepartmentAndAgeUsingSecondarySort extends Configured implements Tool {

	public static class SortByDepartmentAndAgeMapper extends MapReduceBase implements
			Mapper<Object, Text, IntPair, Text> {

		private EmployeeRecordParser parser = new EmployeeRecordParser();

		@Override
		public void map(Object key, Text value, OutputCollector<IntPair, Text> output,
				Reporter reporter) throws IOException {
			parser.parse(value);
			IntPair intPair = new IntPair();
			intPair.set(parser.getDepartmentId(), parser.getEmployeeAge());
			output.collect(intPair, value);
		}
	}

	public static class SortByDepartmentAndAgeReducer extends MapReduceBase implements
			Reducer<IntPair, Text, IntWritable, Text> {

		private EmployeeRecordParser parser = new EmployeeRecordParser();

		@Override
		public void reduce(IntPair key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			while (values.hasNext()) {
				Text value = values.next();
				parser.parse(value);
				output.collect(new IntWritable(parser.getDepartmentId()), value);
			}
		}
	}

	public static class FirstPartitioner implements Partitioner<IntPair, Text> {

		@Override
		public void configure(JobConf job) {
		}

		@Override
		public int getPartition(IntPair key, Text value, int numPartitions) {
			return Math.abs(key.getFirst() * 127) % numPartitions;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setMapperClass(SortByDepartmentAndAgeMapper.class);
		conf.setMapOutputKeyClass(IntPair.class);
		conf.setPartitionerClass(FirstPartitioner.class);
		conf.setOutputValueGroupingComparator(FirstGroupingComparator.class);

		conf.setReducerClass(SortByDepartmentAndAgeReducer.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// 引数を固定で設定
		String in = "input/Employee";
		String out = Util.getJobOutputDirPath(SortByDepartmentAndAgeUsingSecondarySort.class);
		args = new String[] { in, out };
		// 出力先のディレクトリが存在するとFileAlreadyExistsExceptionとなるため事前に削除しています
		FileUtil.fullyDelete(new File(out));

		int res = ToolRunner.run(new SortByDepartmentAndAgeUsingSecondarySort(), args);
		System.exit(res);
	}

}
