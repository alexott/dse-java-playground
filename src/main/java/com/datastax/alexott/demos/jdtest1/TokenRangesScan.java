package com.datastax.alexott.demos.jdtest1;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// create table test.range_scan(id bigint, col1 int, col2 bigint, primary key(id, col1));

public class TokenRangesScan {
    public static void main(String[] args)  {
        Cluster cluster = Cluster.builder()
                .addContactPoint(System.getProperty("contactPoint", "127.0.0.1"))
                .build();
        Session session = cluster.connect();

        Metadata metadata = cluster.getMetadata();
        List<TokenRange> ranges = new ArrayList(metadata.getTokenRanges());
        Collections.sort(ranges);
        System.out.println("Processing " + ranges.size() + " token ranges...");

        long rowCount = 0;
        Token minToken = ranges.get(0).getStart();
        String baseQuery = "SELECT id, col1 FROM test.range_scan WHERE ";
        // Note: It could be speedup by using async queries, but for illustration it's ok
        for (int i = 0; i < ranges.size(); i++) {
            TokenRange range = ranges.get(i);
            Token rangeStart = range.getStart();
            Token rangeEnd = range.getEnd();
            final String whereCond;
            if (i == 0) {
                whereCond = "token(id) <= " +  rangeEnd;
            } else if (rangeEnd.equals(minToken)) {
                whereCond = "token(id) > " +  rangeStart;
            } else {
                whereCond = "token(id) > " + rangeStart + " AND token(id) <= " + rangeEnd;
            }
            SimpleStatement statement = new SimpleStatement(baseQuery + whereCond);
            statement.setRoutingToken(rangeEnd);
            ResultSet rs = session.execute(statement);
            long rangeCount = 0;
            for (Row row: rs) {
                rangeCount++;
            }
            System.out.println("Processed range(" + range.getStart() + "," + rangeEnd + "]. Row count: " + rangeCount);
            rowCount += rangeCount;
        }
        System.out.println("Total row count: " + rowCount);
    }
}
