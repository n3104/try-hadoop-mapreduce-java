package jp.gr.java_conf.n3104.try_mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
 * 従業員ファイルから平均年齢を求めます。
 * <p>
 * Mapper 側で指定した key が同じ場合、 Reducer 側で values に値がまとめられていることを
 * 再確認することを目的としています。今回は従業員全体の平均年齢であるため、
 * key を単一にしています。そのため、 Reducer 側では単一の values に全従業員の年齢が入っています。
 * （key が単一であるため、 reduce メソッドは1回のみ呼び出されます。）
 * </p>
 * 
 * @author n3104
 */
public class AverageAgeOfEmployee extends Configured implements Tool {

	public static class AverageAgeMapper extends MapReduceBase implements
			Mapper<Object, Text, IntWritable, IntWritable> {

		private static final IntWritable MAP_OUTPUT_KEY = new IntWritable(1);

		@Override
		public void map(Object key, Text value, OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {
			String[] values = value.toString().split(",");
			int age = Integer.parseInt(values[2]);
			output.collect(MAP_OUTPUT_KEY, new IntWritable(age));
		}
	}

	public static class AverageAgeReducer extends MapReduceBase implements
			Reducer<IntWritable, IntWritable, NullWritable, DoubleWritable> {

		@Override
		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<NullWritable, DoubleWritable> output, Reporter reporter)
				throws IOException {
			int count = 0;
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
				count++;
			}
			output.collect(NullWritable.get(), new DoubleWritable((double) sum / count));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setMapperClass(AverageAgeMapper.class);
		// ReducerとKeyおよびValueの型が異なる場合はMapperについても型を指定する必要があります。
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setReducerClass(AverageAgeReducer.class);
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// 引数を固定で設定
		String in = "input/Employee";
		String out = Util.getJobOutputDirPath(AverageAgeOfEmployee.class);
		args = new String[] { in, out };
		// 出力先のディレクトリが存在するとFileAlreadyExistsExceptionとなるため事前に削除しています
		FileUtil.fullyDelete(new File(out));

		int res = ToolRunner.run(new AverageAgeOfEmployee(), args);
		System.exit(res);
	}

}
