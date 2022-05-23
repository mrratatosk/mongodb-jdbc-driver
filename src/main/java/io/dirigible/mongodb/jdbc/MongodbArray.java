package io.dirigible.mongodb.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import org.bson.Document;

public class MongodbArray implements Array {

  private final List<Document> documents;

  public MongodbArray(List<Document> documents) {
    this.documents = documents;
  }

  @Override
  public String getBaseTypeName() throws SQLException {
    return "Blob";
  }

  @Override
  public int getBaseType() throws SQLException {
    return Types.BLOB;
  }

  @Override
  public Object getArray() throws SQLException {
    return documents;
  }

  @Override
  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    return getArray();
  }

  @Override
  public Object getArray(long index, int count) throws SQLException {
    return this.documents.subList((int)index, (int)index + count);
  }

  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    return getArray(index, count);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return null;
  }

  @Override
  public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getResultSet(long index, int count) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public void free() throws SQLException {

  }
}
