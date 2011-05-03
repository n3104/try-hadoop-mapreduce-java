package jp.gr.java_conf.n3104.try_mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * hadoop-0.20.2.tar.gz に含まれる {@link org.apache.hadoop.examples.WordCount} をそのままコピーして
 * 引数を固定にして簡単に実行出来るようにしたものです。
 * <p>
 * 動作確認を目的としています。このクラスをJavaアプリケーションとしてEclipse上で実行して、
 * 「target/test/jp/gr/java_conf/n3104/try_mapreduce/WordCountNew」に
 * 「part-r-00000」というファイルが出力されていれば動作確認完了になります。
 * 「part-r-00000」は「src/main/resources」ソースフォルダ内の「jp.gr.java_conf.n3104.try_mapreduce」パッケージ内の
 * 「hadoop-README.txt」をワードカウントした結果のファイルです。
 * </p>
 * 
 * @author n3104
 */
public class WordCountNew {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException,
				InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		// 引数を固定で設定
		String in = WordCountNew.class.getResource("hadoop-README.txt").getPath();
		String out = Util.getJobOutputDirPath(WordCountNew.class);
		args = new String[] { in, out };
		// 出力先のディレクトリが存在するとFileAlreadyExistsExceptionとなるため事前に削除しています
		FileUtil.fullyDelete(new File(out));

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCountNew.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
