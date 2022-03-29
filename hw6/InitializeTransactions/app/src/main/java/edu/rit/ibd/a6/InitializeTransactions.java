package edu.rit.ibd.a6;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;

class Results {
	HashSet<Result> list;

	public Results(ResultSet rs) throws SQLException {
		if (this.list == null) {
			this.list = new HashSet<>();
		}
		while(rs.next()){
			Result r = new Result(rs.getInt("tid"), rs.getInt("iid"));
			this.list.add(r);
		}
	}

	public Results() {
		this.list = new HashSet<Result>();
	}

}

class NewResults {
	HashMap<Integer, ArrayList<Integer>> list;

	public NewResults(ResultSet rs) throws SQLException {
		this.list = new HashMap<>();
		while(rs.next()){
			int tidTemp = rs.getInt("tid");
			if (!list.containsKey(tidTemp)) {
				list.put(tidTemp, new ArrayList<>());
			}
			list.get(tidTemp).add(rs.getInt("iid"));
		}
	}
}

class Result {
	int tid;
	int iid;

	public Result(int tid, int iid ){
		this.tid = tid;
		this.iid = iid;
	}
}

public class InitializeTransactions {

	public static void main(String[] args) throws Exception {
		final String jdbcURL = args[0];
		final String jdbcUser = args[1];
		final String jdbcPwd = args[2];
		final String sqlQuery = args[3];
		final String mongoDBURL = args[4];
		final String mongoDBName = args[5];
		final String mongoCol = args[6];

//		String sqlQ = "";
//
//		sqlQ = "SELECT mid AS tid, pid AS iid FROM Actor JOIN Movie ON id=mid WHERE year BETWEEN 1950 AND 2020 AND totalvotes > 3500 AND mid IN \n" +
//				"\t(SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE \"Sci-Fi\") \n" +
//				"UNION \n" +
//				"SELECT mid AS tid, pid AS iid FROM Director JOIN Movie ON id=mid WHERE year BETWEEN 1950 AND 2020 AND totalvotes > 3500 AND mid IN \n" +
//				"\t(SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE \"Sci-Fi\") \n" +
//				"UNION \n" +
//				"SELECT mid AS tid, pid AS iid FROM Producer JOIN Movie ON id=mid WHERE year BETWEEN 1950 AND 2020 AND totalvotes > 3500 AND mid IN \n" +
//				"\t(SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE \"Sci-Fi\") \n" +
//				"UNION\n" +
//				"SELECT mid AS tid, pid AS iid FROM Writer JOIN Movie ON id=mid WHERE year BETWEEN 1950 AND 2020 AND totalvotes > 3500 AND mid IN \n" +
//				"\t(SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE \"Sci-Fi\") \n";

		Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> transactions = db.getCollection(mongoCol);
		UpdateOptions options = new UpdateOptions().upsert(true);
		
		// TODO Your code here!!!
		PreparedStatement preparedStatement = con.prepareStatement(sqlQuery);
		preparedStatement.setFetchSize(1000);
		ResultSet resultSet = preparedStatement.executeQuery();
		NewResults results = new NewResults(resultSet);
		for (Map.Entry e : results.list.entrySet()) {
			int tid = (int)e.getKey();
			Document d = new Document().append("_id", tid);
			d.append("items", e.getValue());
			transactions.insertOne(d);
		}

		/*
		 * 
		 * Run the input SQL query over the input URL. Remember to use the fetch size to only retrieve a certain number of tuples at a time (useCursorFetch=true will
		 * 	be part of the URL).
		 * 
		 * For each transaction (tid), you need to create a new document and store it in the MongoDB collection specified as input. Such document must contain an array
		 * 	in which the elements are iid lexicographically sorted.
		 * 
		 */
		
		// Run the SQL query to retrieve the data. Recall that it contains two attributes iid and tid, and it is always sorted by tid and, then, iid.
		//	Be mindful of main memory and use an appropriate batch size.
		
		// TODO End of your code!
		
		client.close();
		con.close();
	}
	
	private static MongoClient getClient(String mongoDBURL) {
		MongoClient client = null;
		if (mongoDBURL.equals("None"))
			client = new MongoClient();
		else
			client = new MongoClient(new MongoClientURI(mongoDBURL));
		return client;
	}

}
