package jp.gr.java_conf.n3104.try_mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
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
 * 従業員ファイルと部門ファイルをマージします。
 * <p>
 * 単一の MapReduce プログラムで同時に複数の種類の Mapper を利用する {@link MultipleInputs} のサンプルです。
 * 複数の種類の Mapper の出力をキーでまとめて単一の種類の Reducer で処理することが出来ます。
 * </p>
 * 
 * @author n3104
 */
public class MergeByDepartmentUsingMultipleInputs extends Configured implements Tool {

	public static class EmployeeMapper extends MapReduceBase implements
			Mapper<Object, Text, IntWritable, Text> {

		private EmployeeRecordParser parser = new EmployeeRecordParser();

		@Override
		public void map(Object key, Text value, OutputCollector<IntWritable, Text> output,
				Reporter reporter) throws IOException {
			parser.parse(value);
			output.collect(new IntWritable(parser.getDepartmentId()), value);
		}
	}

	public static class DepartmentMapper extends MapReduceBase implements
			Mapper<Object, Text, IntWritable, Text> {

		private DepartmentRecordParser parser = new DepartmentRecordParser();

		@Override
		public void map(Object key, Text value, OutputCollector<IntWritable, Text> output,
				Reporter reporter) throws IOException {
			parser.parse(value);
			output.collect(new IntWritable(parser.getDepartmentId()),
					new Text(parser.getDepartmentName()));
		}
	}

	public static class MergeByDepartmentReducer extends MapReduceBase implements
			Reducer<IntWritable, Text, IntWritable, Text> {

		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			while (values.hasNext()) {
				output.collect(key, values.next());
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

		conf.setReducerClass(MergeByDepartmentReducer.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// 引数を固定で設定
		String employee = "input/Employee";
		String department = "input/Department";
		String out = Util.getJobOutputDirPath(MergeByDepartmentUsingMultipleInputs.class);
		args = new String[] { employee, department, out };
		// 出力先のディレクトリが存在するとFileAlreadyExistsExceptionとなるため事前に削除しています
		FileUtil.fullyDelete(new File(out));

		int res = ToolRunner.run(new MergeByDepartmentUsingMultipleInputs(), args);
		System.exit(res);
	}

}
