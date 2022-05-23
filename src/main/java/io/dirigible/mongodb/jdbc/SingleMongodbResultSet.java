/**
 * Copyright 2015 Georgi Pavlov
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the License.
 */
package io.dirigible.mongodb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.ParsePosition;
import java.util.Base64;
import java.util.Calendar;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

@Slf4j
public class SingleMongodbResultSet implements ResultSet {

  public static final int RAW_DOCUMENT_INDEX = -100;

  private Document currentDoc;
  private int rowNumber = 0;
  private boolean isClosed;
  private SQLWarning warning;
  private final MongodbResultSetMetaData rsMetadata;

  public SingleMongodbResultSet(String collection, String column, Document document, BsonType type) {
    this.rsMetadata = new MongodbResultSetMetaData(collection);
    this.rsMetadata.addColumn(column, type);
    this.currentDoc = document;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (isWrapperFor(iface)) {
      return (T) this;
    }
    throw new SQLException("No wrapper for " + iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface != null && iface.isAssignableFrom(getClass());
  }

  @Override
  public boolean next() throws SQLException {
    return false;
  }

  @Override
  public void close() throws SQLException {
    this.isClosed = true;
  }

  @Override
  public boolean wasNull() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    if (columnIndex == RAW_DOCUMENT_INDEX) {
      return this.currentDoc.toJson();
    }
    return this.getString(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return this.getBoolean(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return this.getByte(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return this.getShort(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return this.getInt(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return this.getLong(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return this.getFloat(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return this.getDouble(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    return this.getBigDecimal(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return this.getBytes(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return this.getDate(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return this.getTime(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return this.getTimestamp(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return this.getAsciiStream(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return this.getUnicodeStream(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return this.getBinaryStream(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return String.valueOf(this.currentDoc.get(columnLabel));
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return this.currentDoc.getBoolean(columnLabel);
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return Byte.parseByte("" + this.currentDoc.get(columnLabel));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return Short.parseShort("" + this.currentDoc.get(columnLabel));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    if(this.currentDoc.containsKey(columnLabel)) {
      return this.currentDoc.getInteger(columnLabel);
    }

    return -1;
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    if(this.currentDoc.containsKey(columnLabel)) {
      return this.currentDoc.getLong(columnLabel);
    }

    return -1L;
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return Float.parseFloat("" + this.currentDoc.get(columnLabel));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return this.currentDoc.getDouble(columnLabel);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    throw new SQLFeatureNotSupportedException("getBigDecimal - " + columnLabel);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return Base64.getDecoder().decode(this.currentDoc.getString(columnLabel));
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getDate - " + columnLabel);
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getTime - " + columnLabel);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getTimestamp - " + columnLabel);
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getAsciiStream - " + columnLabel);
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getUnicodeStream - " + columnLabel);
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getBinaryStream - " + columnLabel);
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    if (this.isClosed()) {
      throw new SQLException();
    }
    return this.warning;
  }

  @Override
  public void clearWarnings() throws SQLException {
    if (this.isClosed()) {
      throw new SQLException();
    }
    this.warning = null;
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new SQLFeatureNotSupportedException("getDate");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return this.rsMetadata;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return this.getObject(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return this.currentDoc.get(columnLabel);
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("findColumn");
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("getCharacterStream");
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getCharacterStream");
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("getBigDecimal");
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    DecimalFormat df = new DecimalFormat();
    df.setParseBigDecimal(true);
    return (BigDecimal) df.parse(this.currentDoc.getString(columnLabel), new ParsePosition(0));
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return this.currentDoc == null;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    //TODO
    return false;
  }

  @Override
  public boolean isFirst() throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("isFirst");
  }

  @Override
  public boolean isLast() throws SQLException {
    return true;
  }

  @Override
  public void beforeFirst() throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("beforeFirst");
  }

  @Override
  public void afterLast() throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("afterLast");
  }

  @Override
  public boolean first() throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("first");
  }

  @Override
  public boolean last() throws SQLException {
    throw new SQLFeatureNotSupportedException("last");
  }

  @Override
  public int getRow() throws SQLException {
    return this.rowNumber;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    throw new SQLFeatureNotSupportedException("absolute");
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException("relative");
  }

  @Override
  public boolean previous() throws SQLException {
    throw new SQLFeatureNotSupportedException("previous");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLFeatureNotSupportedException("setFetchDirection");
  }

  @Override
  public int getFetchDirection() throws SQLException {
    throw new SQLFeatureNotSupportedException("getFetchDirection");
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException("setFetchSize");
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new SQLFeatureNotSupportedException("getFetchSize");
  }

  @Override
  public int getType() throws SQLException {
    throw new SQLFeatureNotSupportedException("getType");
  }

  @Override
  public int getConcurrency() throws SQLException {
    throw new SQLFeatureNotSupportedException("getConcurrency");
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    throw new SQLFeatureNotSupportedException("rowUpdated");
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw new SQLFeatureNotSupportedException("rowInserted");
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw new SQLFeatureNotSupportedException("rowDeleted");
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateNull");
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBoolean");
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateByte");
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateShort");
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateInt");
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateLong");
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateFloat");
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateDouble");
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBigDecimal");
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateString");
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBytes");
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateDate");
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateTime");
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateTimestamp");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateAsciiStream");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBinaryStream");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateCharacterStream");
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateObject");
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateObject");
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateNull");
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBoolean");
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateByte");
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateShort");
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateInt");
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateLong");
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateFloat");
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateDouble");
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBigDecimal");
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateString");
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBytes");
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateDate");
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateTime");
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateTimestamp");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateAsciiStream");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBinaryStream");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateCharacterStream");
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateObject");
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateObject");
  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("insertRow");
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("updateRow");
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("deleteRow");
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("refreshRow");
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException("cancelRowUpdates");
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("moveToInsertRow");
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("moveToCurrentRow");
  }

  @Override
  public Statement getStatement() throws SQLException {
    return null;
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("getObject");
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("getRef");
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("getBlob");
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("getClob");
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    return this.getArray(this.rsMetadata.getColumnLabel(columnIndex));
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getObject");
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getRef");
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getBlob");
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getClob");
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    if(this.currentDoc.containsKey(columnLabel)) {
      log.info(this.currentDoc.get(columnLabel).toString());

    }
    return null;
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("getDate");
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getDate");
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("getTime");
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getTime");
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("getTimestamp");
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getTimestamp");
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("getURL");
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    try {
      return new URL(this.currentDoc.getString(columnLabel));
    } catch (MalformedURLException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateRef");
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("getRowId");
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getRowId");
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("getHoldability");
  }

  @Override
  public boolean isClosed() throws SQLException {
    return this.isClosed;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("getNClob");
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getNClob");
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("getSQLXML");
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getSQLXML");
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("getNString");
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getNString");
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("getNCharacterStream");
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getNCharacterStream");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    throw new SQLFeatureNotSupportedException("getObject");
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    //TODO
    throw new SQLFeatureNotSupportedException("getObject");
  }

  public int getFields() {
    // TODO Auto-generated method stub
    return 0;
  }

}
