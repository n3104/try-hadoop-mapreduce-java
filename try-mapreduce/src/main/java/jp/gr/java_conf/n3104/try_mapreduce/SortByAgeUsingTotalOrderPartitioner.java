package jp.gr.java_conf.n3104.try_mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 従業員ファイルを年齢順にソートします。
 * <p>
 * 全体ソートのサンプルです。 Reducer の出力全体としてソートを行うには、
 * 予め Mapper から Reducer に値を転送する際に全体としてソート出来るように
 * Reducer 毎が受け持つ key の範囲を制御する必要があります。
 * 例えば、Reducer1 は 1～100、 Reducer2 は 101～200、 Reducer3 は 201～300 のような制御です。
 * この制御を行うのが {@link TotalOrderPartitioner} です。 {@link TotalOrderPartitioner} を利用することで
 * 全体ソートを行うことが出来るようになります。
 * <p>
 * Partitioner とは Mapper の出力をどの Reducer に振り分けるか制御するクラスを指します。
 * 部分のソートのサンプルである {@link SortByAgeUsingHashPartitioner} の場合は
 * デフォルトの {@link HashPartitioner} を利用しています。
 * この Partitioner は key の hash 値に基づいて Reducer を決定する実装となっており、
 * 全体ソートには利用できません。
 * </p>
 * <p> {@link TotalOrderPartitioner} を利用する際は、どの Reducer にどの key を割り当てるかの設定を
 * 予め指定しておく必要があります。なお、実データを分析しなければ key の偏りは判別できないため、
 * Hadoop では予めいくつかの {@link InputSampler} が用意されています。
 * この {@link InputSampler} を利用して実データのサンプリングを行います。
 * </p>
 * <p> {@link InputSampler} は入力ファイルの key をサンプリングしてパーティション情報を作成します。
 * そのため、 {@link InputSampler} を利用するジョブの入力ファイルの key は年齢である必要があります。
 * 従業員ファイルをそのまま入力ファイルとした場合、 key にはファイルのバイトオフセットが設定されるため、
 * 年齢によるソート用のパーティション情報を作成することが出来ません。
 * そのため、事前に従業員ファイルを元に key を年齢とする SequenceFile を作成するジョブを実行しています。
 * </p>
 * <p> {@link InputSampler} 利用して作成したパーティション情報は Hadoop クラスタ上の
 * 全 Mapper に転送される必要があります。その転送処理に利用するのが {@link DistributedCache} です。 {@link DistributedCache}
 * を利用することで Hadoop クラスタ上の各タスクノードに
 * 任意のファイルを転送することが出来ます。
 * </p>
 * 
 * @author n3104
 */
public class SortByAgeUsingTotalOrderPartitioner extends Configured implements Tool {

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

	@Override
	public int run(String[] args) throws Exception {
		Path keyValuedFileDir = new Path(args[1] + "/KeyValue");
		Path outputDir = new Path(args[1] + "/Result");
		{
			// 従業員ファイルから key=年齢, value=1行 のSequenceFileを作成
			JobConf conf = new JobConf(getConf(), getClass());
			conf.setMapperClass(SortByAgeMapper.class);
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(Text.class);
			conf.setOutputFormat(SequenceFileOutputFormat.class);
			FileInputFormat.addInputPath(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, keyValuedFileDir);
			JobClient.runJob(conf);
		}
		{
			// TotalOrderPartitionerを利用してソート
			JobConf conf = new JobConf(getConf(), getClass());
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(Text.class);
			conf.setInputFormat(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(conf, keyValuedFileDir);
			FileOutputFormat.setOutputPath(conf, outputDir);
			// TotalOrderPartitionerを利用して全体ソートを行います。
			conf.setPartitionerClass(TotalOrderPartitioner.class);
			// InputSamplerを利用してパーティション情報を生成します。
			InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(
					0.1, 10000, 10);
			Path input = FileInputFormat.getInputPaths(conf)[0];
			input = input.makeQualified(input.getFileSystem(conf));
			Path partitionFile = new Path(input, "_partitions");
			TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
			InputSampler.writePartitionFile(conf, sampler);
			// DistributedCacheを利用してパーティション情報をクラスタ全体に転送します。
			URI partitionUri = new URI(partitionFile.toString() + "#_partitions");
			DistributedCache.addCacheFile(partitionUri, conf);
			DistributedCache.createSymlink(conf);
			JobClient.runJob(conf);
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// 引数を固定で設定
		String in = "input/Employee";
		String out = Util.getJobOutputDirPath(SortByAgeUsingTotalOrderPartitioner.class);
		args = new String[] { in, out };
		// 出力先のディレクトリが存在するとFileAlreadyExistsExceptionとなるため事前に削除しています
		FileUtil.fullyDelete(new File(out));

		int res = ToolRunner.run(new SortByAgeUsingTotalOrderPartitioner(), args);
		System.exit(res);
	}

}
