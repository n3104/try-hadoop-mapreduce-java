package jp.gr.java_conf.n3104.try_mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/**
 * 部門ファイルを解析します。
 * 
 * @author n3104
 */
public class DepartmentRecordParser {

	private static final Log log = LogFactory.getLog(DepartmentRecordParser.class);

	private int departmentId;
	private String departmentName;
	private boolean isValid;

	public void parse(String record) {
		init();
		try {
			String[] values = record.split(",");
			if (values.length != 2) {
				log.debug(String.format("要素数が不正です。values.length=%d", values.length));
				return;
			}
			int i = 0;
			departmentId = Integer.parseInt(values[i++]);
			departmentName = values[i++];
			isValid = true;
		} catch (Exception e) {
			log.debug("解析に失敗しました。", e);
		}
	}

	public void parse(Text record) {
		parse(record.toString());
	}

	private void init() {
		departmentId = 0;
		departmentName = null;
		isValid = false;
	}

	public int getDepartmentId() {
		return departmentId;
	}

	public String getDepartmentName() {
		return departmentName;
	}

	public boolean isValid() {
		return isValid;
	}
}
