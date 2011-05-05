package jp.gr.java_conf.n3104.try_mapreduce;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * ユーティリティです。
 * 
 * @author n3104
 */
public class Util {

	private Util() {
	}

	/**
	 * 指定されたクラスからジョブの出力結果ディレクトリのパスを生成して返します。
	 * 
	 * @param clazz クラス
	 * @return ジョブの出力結果ディレクトリのパス
	 */
	public static String getJobOutputDirPath(Class<?> clazz) {
		String packagePath = clazz.getPackage().getName().replace('.', '/');
		return "target/test/" + packagePath + "/" + clazz.getSimpleName();

	}

	/**
	 * 指定されたクラスからジョブの出力結果ディレクトリのパスを生成して返します。
	 * <p>
	 * パスの最後にタイムスタンプを付与するため、前回の処理結果を保持したい際に利用してください。
	 * </p>
	 * 
	 * @param clazz クラス
	 * @return タイムスタンプ付きのジョブの出力結果ディレクトリのパス
	 */
	public static String getJobOutputDirPathWithTimestamp(Class<?> clazz) {
		String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		return getJobOutputDirPath(clazz) + "_" + timestamp;
	}

}
