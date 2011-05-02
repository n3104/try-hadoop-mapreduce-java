package jp.gr.java_conf.n3104.try_mapreduce;

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
		return "target/test/" + clazz.getPackage().getName().replace('.', '/') + "/"
				+ clazz.getSimpleName();

	}

}
