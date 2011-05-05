package jp.gr.java_conf.n3104.try_mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/**
 * Employee.txt を解析します。
 * 
 * @author n3104
 */
public class EmployeeRecordParser {

	private static final Log log = LogFactory.getLog(EmployeeRecordParser.class);

	private int employeeId;
	private String name;
	private int age;
	private int salary;
	private int departmentId;
	private boolean isValid;

	public void parse(String record) {
		init();
		try {
			String[] values = record.split(",");
			if (values.length != 5) {
				log.debug(String.format("要素数が不正です。values.length=%d", values.length));
				return;
			}
			int i = 0;
			employeeId = Integer.parseInt(values[i++]);
			name = values[i++];
			age = Integer.parseInt(values[i++]);
			salary = Integer.parseInt(values[i++]);
			departmentId = Integer.parseInt(values[i++]);
			isValid = true;
		} catch (Exception e) {
			log.debug("解析に失敗しました。", e);
		}
	}

	public void parse(Text record) {
		parse(record.toString());
	}

	private void init() {
		employeeId = 0;
		name = null;
		age = 0;
		salary = 0;
		departmentId = 0;
		isValid = false;
	}

	public int getEmployeeId() {
		return employeeId;
	}

	public String getName() {
		return name;
	}

	public int getAge() {
		return age;
	}

	public int getSalary() {
		return salary;
	}

	public int getDepartmentId() {
		return departmentId;
	}

	public boolean isValid() {
		return isValid;
	}
}
