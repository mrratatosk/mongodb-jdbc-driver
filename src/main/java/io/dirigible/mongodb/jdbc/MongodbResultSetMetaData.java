/**
 * 	Copyright 2015 Georgi Pavlov
 *
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use this file except in compliance with the License.
 *	You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *	Unless required by applicable law or agreed to in writing, software
 *	distributed under the License is distributed on an "AS IS" BASIS,
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 */
package io.dirigible.mongodb.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import lombok.extern.slf4j.Slf4j;
import org.bson.BsonType;
import org.bson.codecs.BsonTypeClassMap;

@Slf4j
public class MongodbResultSetMetaData implements ResultSetMetaData {

	private SortedMap<String, BsonType> keyMap = new TreeMap<>();
	private List<String> columnsOrder = new ArrayList<>();
	private final String collectionName;
	private BsonTypeClassMap bsonTojavaTypeMap = new BsonTypeClassMap();

	public MongodbResultSetMetaData(String collectionName){
		 this.collectionName =  collectionName;
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

	public void addColumn(String columnName, BsonType type) {
		if(!keyMap.containsKey(columnName)) {
			this.keyMap.put(columnName, type);
			this.columnsOrder.add(columnName);
			log.debug("Register column [" + this.columnsOrder.size() + "] " + columnName + " with type: " + type.toString());
		}
	}
	
	@Override
	public int getColumnCount() throws SQLException {
		return this.columnsOrder.size();
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException {
		return columnNullableUnknown;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		return false;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		return 25;
	}

	@SuppressWarnings("unchecked")
	@Override
	public String getColumnLabel(int column) throws SQLException {
		log.debug("Read column: " + column);
		return columnsOrder.get(column - 1);
	}

	@SuppressWarnings("unchecked")
	@Override
	public String getColumnName(int column) throws SQLException {
		log.debug("Read column: " + column);
		return columnsOrder.get(column - 1);
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		return null;
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException {
		return 0;
	}

	@Override
	public String getTableName(int column) throws SQLException {
		return this.collectionName;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		return null;
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		return this.getSqlType(this.keyMap.get(columnsOrder.get(column - 1)));
	}

	@SuppressWarnings("unchecked")
	@Override
	public String getColumnTypeName(int column) throws SQLException {
		return this.getSqlTypeName(this.keyMap.get(columnsOrder.get(column - 1)));
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return false;
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		return this.bsonTojavaTypeMap.get(this.keyMap.get(columnsOrder.get(column - 1))).getCanonicalName();
	}
	
	int getSqlType(BsonType bsonType){
		switch(bsonType){
			case OBJECT_ID: { return Types.OTHER;}
			case ARRAY: { return Types.ARRAY;}
			case BINARY: { return Types.BINARY; }
			case BOOLEAN: { return Types.BOOLEAN;}
			/*case DATE_TIME: { return Types.DATE}*/
			case DOCUMENT: { return Types.OTHER; }
			case DOUBLE: { return Types.DOUBLE; }
			case INT32: {return Types.INTEGER; }
			case INT64: {return Types.BIGINT; }
			case STRING: { return Types.VARCHAR; }
			case TIMESTAMP: { return Types.TIMESTAMP;}
			default: break;
		}
		return Integer.MIN_VALUE;
	}
	
	String getSqlTypeName(BsonType bsonType){
		switch(bsonType){
			case ARRAY: { return "ARRAY";}
			case BINARY: { return "BINARY"; }
			case BOOLEAN: { return "BOOLEAN";}
			/*case DATE_TIME: { return Types.DATE}*/
			case DOCUMENT: { return "OTHER"; }
			case DOUBLE: { return "DOUBLE"; }
			case INT32: {return "INTEGER"; }
			case INT64: {return "BIGINT"; }
			case STRING: { return "VARCHAR"; }
			case TIMESTAMP: { return "TIMESTAMP";}
			case OBJECT_ID: { return "OTHER";}
			default: break;
		}
		return null;
	}
	
}
