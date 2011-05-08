package jp.gr.java_conf.n3104.try_mapreduce;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

public class UtilTest {

	@Test
	public void getJobOutputDirPath() throws Exception {
		assertThat(Util.getJobOutputDirPath(UtilTest.class), is("target/output/UtilTest"));
		assertThat(Util.getJobOutputDirPath(String.class), is("target/output/String"));
	}

	@Test
	public void getJobOutputDirPathWithTimestamp() throws Exception {
		System.out.println(Util.getJobOutputDirPathWithTimestamp(UtilTest.class));
	}

}
