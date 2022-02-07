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

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoIterable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;

import java.util.List;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;

public class MongodbStatement implements Statement {
	
	protected MongodbConnection conn;
	protected boolean isClosed = false;
	
	public MongodbStatement(MongodbConnection conn){
		this.conn = conn;
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

	/**
	 * Input string: the document specification as defined in https://docs.mongodb.org/manual/reference/command/find/#dbcmd.find
	 */
	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		MongoDatabase db = this.conn.getMongoDb();
		BsonDocument query = null;
		if(sql==null || sql.length()<1)//that is a call to find() in terms of mongodb queries
			query = new BsonDocument();
		else
			query = BsonDocument.parse(sql);

		if((query.containsKey("filter") && query.containsKey("aggreg")) || (!query.containsKey("filter") && !query.containsKey("aggreg"))) {
			throw new IllegalArgumentException("Specify either a find or an aggreg field");
		}

		MongoIterable<Document> searchHits = null;
		String collectionName = query.containsKey("find") ? query.getString("find").getValue() : null;
		if (collectionName == null) {
			collectionName = this.conn.getCollectionName();//fallback if any
		}

		if (collectionName == null) {
			throw new IllegalArgumentException("Specifying a collection is mandatory for query operations");
		}

		if(query.containsKey("filter")) {
			BsonDocument filter = query.containsKey("filter") ? query.getDocument("filter") : null;

			if (filter == null) {
				searchHits = db.getCollection(collectionName).find();
			} else {
				searchHits = db.getCollection(collectionName).find(filter);
			}
			if (query.containsKey("batchSize")) {
				searchHits.batchSize(query.getInt32("batchSize").getValue());
			}

			if (query.containsKey("limit")) {
				((FindIterable<Document>) searchHits).limit(query.getInt32("limit").getValue());
			}

			if (query.containsKey("sort")) {
				((FindIterable<Document>) searchHits).sort(query.getDocument("sort"));
			}
		} else if(query.containsKey("aggreg")) {
			List<Bson> aggreg = query.containsKey("aggreg") ? query.getArray("aggreg").stream().map(BsonValue::asDocument).collect(Collectors.toList()) : null;

			searchHits = db.getCollection(collectionName).aggregate(aggreg).allowDiskUse(true);

			if (query.containsKey("batchSize")) {
				searchHits.batchSize(query.getInt32("batchSize").getValue());
			}
		}
		return new MongodbResultSet(this, searchHits);
	}

	/**
	 * https://docs.mongodb.org/manual/reference/command/update/#dbcmd.update
	 */
	@Override
	public int executeUpdate(String sql) throws SQLException {
		BsonDocument updateDocument = null;
		if(sql==null || sql.length()<1)
			throw new IllegalArgumentException();
		else
			updateDocument = BsonDocument.parse(sql);
		
		Document response = this.conn.getMongoDb().runCommand(updateDocument);
		int updatedDocuments = 0;
		if(response!=null && response.get("ok")!=null){
			updatedDocuments = response.getInteger("nModified");
			//TODO operation atomicity concerns? /errors/
		}
		return updatedDocuments;
	}

	@Override
	public void close() throws SQLException {
		this.isClosed = true;
		this.conn.close();
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		// TODO Auto-generated method stub
	}

	@Override
	public int getMaxRows() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		// TODO Auto-generated method stub
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		// TODO Auto-generated method stub
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		// TODO Auto-generated method stub
	}

	@Override
	public void cancel() throws SQLException {
		// TODO Auto-generated method stub
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		// TODO Auto-generated method stub
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getUpdateCount() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchDirection() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchSize() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetType() throws SQLException {
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearBatch() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public int[] executeBatch() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Connection getConnection() throws SQLException {
		return this.conn;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return this.isClosed;
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isPoolable() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

}
