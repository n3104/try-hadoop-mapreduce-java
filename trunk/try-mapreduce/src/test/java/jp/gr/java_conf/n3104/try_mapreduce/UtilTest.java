package jp.gr.java_conf.n3104.try_mapreduce;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

public class UtilTest {

	@Test
	public void getJobOutputDirPath() throws Exception {
		assertThat(Util.getJobOutputDirPath(UtilTest.class),
				is("target/test/jp/gr/java_conf/n3104/try_mapreduce/UtilTest"));
		assertThat(Util.getJobOutputDirPath(String.class), is("target/test/java/lang/String"));
	}

	@Test
	public void getJobOutputDirPathWithTimestamp() throws Exception {
		System.out.println(Util.getJobOutputDirPathWithTimestamp(UtilTest.class));
	}

}
