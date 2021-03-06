package jp.gr.java_conf.n3104.try_mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import jp.gr.java_conf.n3104.try_mapreduce.SortByDeptAndAgeUsingSecondarySort.FirstPartitioner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.SecondarySort.FirstGroupingComparator;
import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Reduce-Side Join アルゴリズムを利用して、従業員ファイルに部門名をジョインします。
 * <p>
 * Reduce-Side Join は {@link MultipleInputs} と Secondary Sort によって構成されています。 {@link MultipleInputs}
 * を利用してジョインする複数の種類のファイルを入力として受け取り、
 * Secondary Sort を利用して Reducer で受け取るデータの順番を制御します。
 * このプログラムの場合は、 Reducer で受け取る values の先頭に部門名が来るようにして、
 * それ以降の値は従業員レコードになるようにしています。
 * </p>
 * 
 * @author n3104
 */
public class JoinWithDeptNameUsingReduceSideJoin extends Configured implements Tool {

	public static class EmployeeMapper extends MapReduceBase implements
			Mapper<Object, Text, IntPair, Text> {

		private static final int EMPLOYEE_KEY = 2;

		private EmployeeRecordParser parser = new EmployeeRecordParser();

		@Override
		public void map(Object key, Text value, OutputCollector<IntPair, Text> output,
				Reporter reporter) throws IOException {
			parser.parse(value);
			IntPair intPair = new IntPair();
			intPair.set(parser.getDepartmentId(), EMPLOYEE_KEY);
			output.collect(intPair, value);
		}
	}

	public static class DepartmentMapper extends MapReduceBase implements
			Mapper<Object, Text, IntPair, Text> {

		private static final int DEPARTMENT_KEY = 1;

		private DepartmentRecordParser parser = new DepartmentRecordParser();

		@Override
		public void map(Object key, Text value, OutputCollector<IntPair, Text> output,
				Reporter reporter) throws IOException {
			parser.parse(value);
			IntPair intPair = new IntPair();
			intPair.set(parser.getDepartmentId(), DEPARTMENT_KEY);
			output.collect(intPair, new Text(parser.getDepartmentName()));
		}
	}

	public static class JoinWithDepartmentNameReducer extends MapReduceBase implements
			Reducer<IntPair, Text, IntWritable, Text> {

		@Override
		public void reduce(IntPair key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			IntWritable departmentId = new IntWritable(key.getFirst());
			String departmentName = values.next().toString();
			while (values.hasNext()) {
				Text outValue = new Text(departmentName + "," + values.next().toString());
				output.collect(departmentId, outValue);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), getClass());

		MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class,
				EmployeeMapper.class);
		MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class,
				DepartmentMapper.class);
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		conf.setMapOutputKeyClass(IntPair.class);
		conf.setPartitionerClass(FirstPartitioner.class);
		conf.setOutputValueGroupingComparator(FirstGroupingComparator.class);

		conf.setReducerClass(JoinWithDepartmentNameReducer.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// 引数を固定で設定
		String employee = "input/Employee";
		String department = "input/Department";
		String out = Util.getJobOutputDirPath(JoinWithDeptNameUsingReduceSideJoin.class);
		args = new String[] { employee, department, out };
		// 出力先のディレクトリが存在するとFileAlreadyExistsExceptionとなるため事前に削除しています
		FileUtil.fullyDelete(new File(out));

		int res = ToolRunner.run(new JoinWithDeptNameUsingReduceSideJoin(), args);
		System.exit(res);
	}

}
