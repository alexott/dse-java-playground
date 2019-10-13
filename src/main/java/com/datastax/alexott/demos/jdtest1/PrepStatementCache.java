package com.datastax.alexott.demos.jdtest1;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import java.util.concurrent.ConcurrentHashMap;

public class PrepStatementCache {
  private static ConcurrentHashMap<String, PreparedStatement> cache = new ConcurrentHashMap<>();

  static PreparedStatement getStatement1(Session session, final String query) {
    return cache.computeIfAbsent(query,  q -> session.prepare(query));
  }

  static PreparedStatement getStatement2(Session session, final String query) {
    PreparedStatement preparedStatement = cache.get(query);
    if (preparedStatement == null) {
      preparedStatement = session.prepare(query);
      if (preparedStatement != null) {
        PreparedStatement p2 = cache.putIfAbsent(query, preparedStatement);
        preparedStatement = p2 == null ? preparedStatement : p2;
      }
    }

    return preparedStatement;
  }

}
