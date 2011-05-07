package jp.gr.java_conf.n3104.try_mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 分散キャッシュファイルを利用して、従業員ファイルに部門名をジョインします。
 * 
 * @author n3104
 */
public class JoinWithDeptNameUsingDistributedCacheFile extends Configured implements Tool {

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

	public static class JoinWithDepartmentNameReducer extends MapReduceBase implements
			Reducer<IntWritable, Text, IntWritable, Text> {

		private Map<Integer, String> departmentMap = new HashMap<Integer, String>();

		@Override
		public void configure(JobConf job) {
			try {
				String departmentFile = "Department.txt";
				DepartmentRecordParser parser = new DepartmentRecordParser();
				for (String record : FileUtils.readLines(new File(departmentFile), "UTF-8")) {
					parser.parse(record);
					departmentMap.put(parser.getDepartmentId(), parser.getDepartmentName());
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			String departmentName = departmentMap.get(key.get());
			while (values.hasNext()) {
				Text outValue = new Text(departmentName + "," + values.next().toString());
				output.collect(key, outValue);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setMapperClass(EmployeeMapper.class);
		conf.setReducerClass(JoinWithDepartmentNameReducer.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// 引数を固定で設定
		String files = "-files";
		String distributedCacheFile = "input/Department/Department.txt";
		String in = "input/Employee";
		String out = Util.getJobOutputDirPath(JoinWithDeptNameUsingDistributedCacheFile.class);
		args = new String[] { files, distributedCacheFile, in, out };
		// 出力先のディレクトリが存在するとFileAlreadyExistsExceptionとなるため事前に削除しています
		FileUtil.fullyDelete(new File(out));
		// LocalJobRunnerはdistributedCacheFileを利用できないため、作業ディレクトリ直下にファイルを自前でコピーしています
		File pseudoSymLink = new File(new File(distributedCacheFile).getName());
		pseudoSymLink.deleteOnExit();
		FileUtils.copyFile(new File(distributedCacheFile), pseudoSymLink);

		int res = ToolRunner.run(new JoinWithDeptNameUsingDistributedCacheFile(), args);
		System.exit(res);
	}

}
