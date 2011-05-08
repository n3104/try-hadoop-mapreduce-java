package jp.gr.java_conf.n3104.try_mapreduce;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Map-Side Join アルゴリズムを利用して、従業員ファイルに部門名をジョインします。
 * <p>
 * Reduce-Side Join を利用する際は、予めジョインするファイル間を同じキーでソートしておく必要があります。
 * この事前準備を行っておけば {@link CompositeInputFormat} を利用することでファイルをジョインすることが出来ます。
 * なお、このプログラムでは事前準備を行うため、ジョイン用のジョブの他に、
 * 従業員ファイルと部門ファイルを {@code departmentId} でソートするジョブを実行しています。
 * そのため、単一のプログラム内で3つの MapReduce ジョブを実行しています。
 * </p>
 * 
 * @author n3104
 */
public class JoinWithDeptNameUsingMapSideJoin extends Configured implements Tool {

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

	@Override
	public int run(String[] args) throws Exception {
		Path sortedEmployeeFileDir = new Path(args[2] + "/Employee");
		Path sortedDepartmentFileDir = new Path(args[2] + "/Department");
		Path outputDir = new Path(args[2] + "/Result");
		{
			// 従業員ファイルを部門でソート
			JobConf conf = new JobConf(getConf(), getClass());
			conf.setMapperClass(EmployeeMapper.class);
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, sortedEmployeeFileDir);
			JobClient.runJob(conf);
		}
		{
			// 部門ファイルを部門でソート
			JobConf conf = new JobConf(getConf(), getClass());
			conf.setMapperClass(DepartmentMapper.class);
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(conf, new Path(args[1]));
			FileOutputFormat.setOutputPath(conf, sortedDepartmentFileDir);
			JobClient.runJob(conf);
		}
		{
			// ソートされた従業員ファイルと部門ファイルをジョイン
			JobConf conf = new JobConf(getConf(), getClass());
			conf.setInputFormat(CompositeInputFormat.class);
			FileInputFormat.addInputPath(conf, sortedDepartmentFileDir);
			FileInputFormat.addInputPath(conf, sortedEmployeeFileDir);
			conf.set("mapred.join.expr", CompositeInputFormat.compose("inner",
					KeyValueTextInputFormat.class, FileInputFormat.getInputPaths(conf)));
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(TupleWritable.class);
			FileOutputFormat.setOutputPath(conf, outputDir);
			JobClient.runJob(conf);
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// 引数を固定で設定
		String employee = "input/Employee";
		String department = "input/Department";
		String out = Util.getJobOutputDirPath(JoinWithDeptNameUsingMapSideJoin.class);
		args = new String[] { employee, department, out };
		// 出力先のディレクトリが存在するとFileAlreadyExistsExceptionとなるため事前に削除しています
		FileUtil.fullyDelete(new File(out));

		int res = ToolRunner.run(new JoinWithDeptNameUsingMapSideJoin(), args);
		System.exit(res);
	}

}
