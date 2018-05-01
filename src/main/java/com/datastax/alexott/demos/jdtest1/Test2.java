package com.datastax.alexott.demos.jdtest1;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class Test2 {
	public static void main(String[] args) throws JsonProcessingException {
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		Session session = cluster.connect();

		ResultSet rs = session.execute("select json * from test.jtest ;");
		int i = 0;
		System.out.print("[");
		for (Row row : rs) {
			if (i > 0)
				System.out.print(",");
			i++;
			String json = row.getString(0);
			System.out.print(json);
		}
		System.out.println("]");

		ObjectMapper mapper = new ObjectMapper();
		SimpleModule module = new SimpleModule();
		module.addSerializer(ResultSet.class, new ResultSetSerializer());
		mapper.registerModule(module);

		rs = session.execute("select * from test.jtest ;");
		String json = mapper.writeValueAsString(rs);
		System.out.println(json);

		session.close();
		cluster.close();
	}

}
