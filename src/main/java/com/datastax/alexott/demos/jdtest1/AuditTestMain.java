package com.datastax.alexott.demos.jdtest1;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.Map;

public class AuditTestMain {
    public static void main(String[] args) {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect();

        MappingManager manager = new MappingManager(session);
        Mapper<AuditTestTable> mapper = manager.mapper(AuditTestTable.class);

        Map<Integer, String> m = Maps.newHashMap();
        m.put(1, "m 1");
        m.put(2, "m 2");
        mapper.save(new AuditTestTable(2, new AuditTestType(2, "test 2"),
                Sets.newHashSet("s 1", " s 2"), Lists.newArrayList("l 1", "l 2"),
                m));

        System.out.println(mapper.get(2));

        session.close();
        cluster.close();
    }

}
