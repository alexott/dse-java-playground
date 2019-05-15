package com.datastax.alexott.demos.jdtest1;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.apache.cassandra.batchlog.Batch;

import java.util.Random;

public class Test4_2 {
    public static void main(String[] args) throws InterruptedException {

        try(Cluster cluster = Cluster.builder().withProtocolVersion(ProtocolVersion.V4)
                .addContactPoint(System.getProperty("contactPoint", "127.0.0.1")).build();
            Session session = cluster.connect()) {
            MappingManager manager = new MappingManager(session);
            Mapper<Test4Data> mapper = manager.mapper(Test4Data.class);

            Random rnd = new Random();

            while(true) {
                int i = rnd.nextInt();
                int j = rnd.nextInt();
                mapper.save(new Test4Data(i, j, "t " + i + "," + j));
                Thread.sleep(200);
            }
        }
    }

}
