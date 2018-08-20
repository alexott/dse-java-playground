package com.datastax.alexott.demos.btest;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

public class BTest {
	public static void main(String[] args) {
		try (Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
				Session session = cluster.connect()) {
			session.execute(
					"create KEYSPACE if not exists test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
			session.execute("create table if not exists test.btest (id int primary key, txt text, u timeuuid);");

			PreparedStatement pStmt = session.prepare("insert into test.btest(id, txt, u) values (?,?,now());");
			pStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);

			for (int i = 0; i < 10; i++) {
				BoundStatement st = pStmt.bind(i, "txt " + i);
				session.execute(st);
			}
			ResultSet rs = session.execute("select id, txt, u from test.btest limit 2");

			for (Row row : rs) {
				System.out.println("id=" + row.getInt(0) + ", text='" + row.getString(1) + "', uuid=" + row.getUUID(2));
			}

			MappingManager manager = new MappingManager(session);
			Mapper<BData> mapper = manager.mapper(BData.class);

			BData b = mapper.get(0);
			if (b != null) {
				System.out.println("Got data from mapper: " + b);
			}

		}
	}
}
