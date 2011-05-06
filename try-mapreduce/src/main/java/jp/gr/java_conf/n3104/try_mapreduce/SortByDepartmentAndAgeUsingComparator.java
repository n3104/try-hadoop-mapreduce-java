package jp.gr.java_conf.n3104.try_mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

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
 * 従業員ファイルを部門と年齢でソートします。
 * <p>
 * Mapper のキーを {@code demartmentId} にして部門単位でまずソートし、
 * Reducer 内で {@link Comparator} を利用して年齢でソートしています。
 * </p>
 * 
 * @author n3104
 */
public class SortByDepartmentAndAgeUsingComparator extends Configured implements Tool {

	public static class SortByDepartmentAndAgeMapper extends MapReduceBase implements
			Mapper<Object, Text, IntWritable, Text> {

		private EmployeeRecordParser parser = new EmployeeRecordParser();

		@Override
		public void map(Object key, Text value, OutputCollector<IntWritable, Text> output,
				Reporter reporter) throws IOException {
			parser.parse(value);
			output.collect(new IntWritable(parser.getDepartmentId()), value);
		}
	}

	public static class SortByDepartmentAndAgeReducer extends MapReduceBase implements
			Reducer<IntWritable, Text, IntWritable, Text> {

		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			List<Text> list = new ArrayList<Text>();
			while (values.hasNext()) {
				// Iteratorの要素は参照が再利用される実装であるため、別途Textをnewする必要がある。
				list.add(new Text(values.next()));
			}
			// ageでソート
			Collections.sort(list, new Comparator<Text>() {

				private EmployeeRecordParser parser1 = new EmployeeRecordParser();
				private EmployeeRecordParser parser2 = new EmployeeRecordParser();

				@Override
				public int compare(Text o1, Text o2) {
					parser1.parse(o1);
					parser2.parse(o2);
					return Integer.valueOf(parser1.getAge()).compareTo(parser2.getAge());
				}

			});
			for (Text value : list) {
				output.collect(key, value);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setMapperClass(SortByDepartmentAndAgeMapper.class);
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
		String out = Util.getJobOutputDirPath(SortByDepartmentAndAgeUsingComparator.class);
		args = new String[] { in, out };
		// 出力先のディレクトリが存在するとFileAlreadyExistsExceptionとなるため事前に削除しています
		FileUtil.fullyDelete(new File(out));

		int res = ToolRunner.run(new SortByDepartmentAndAgeUsingComparator(), args);
		System.exit(res);
	}

}
