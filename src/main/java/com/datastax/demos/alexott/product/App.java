package com.datastax.demos.alexott.product;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

public class App {

	public static void main(String[] args) {
		CodecRegistry codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withCodecRegistry(codecRegistry).build();
		Session session = cluster.connect();

		MappingManager manager = new MappingManager(session);
		Mapper<Product> mapper = manager.mapper(Product.class);
		Product product = mapper.get("test");
		System.out.println("Product: " + product);
		
		session.close();
	}

}
