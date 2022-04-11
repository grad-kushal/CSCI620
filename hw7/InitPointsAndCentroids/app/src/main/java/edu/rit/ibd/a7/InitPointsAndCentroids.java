package edu.rit.ibd.a7;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.bson.Document;
import org.bson.types.Decimal128;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class InitPointsAndCentroids {
	public enum Scaling {None, MinMax, Mean, ZScore}

	public static void main(String[] args) throws Exception {
		final String jdbcURL = args[0];
		final String jdbcUser = args[1];
		final String jdbcPwd = args[2];
		final String sqlQuery = args[3];
		final String mongoDBURL = args[4];
		final String mongoDBName = args[5];
		final String mongoCol = args[6];
		final Scaling scaling = Scaling.valueOf(args[7]);
		final int k = Integer.valueOf(args[8]);
		
		Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> collection = db.getCollection(mongoCol);

		//System.out.println(jdbcURL + " \n" + sqlQuery + " \n" + mongoCol + " \n" + scaling + "\n" + k);

//		SELECT CAST(CONCAT(mid, pid) AS UNSIGNED) AS id, year AS dim_0, rating AS dim_1, byear AS dim_2, runtime AS dim_4, totalvotes AS dim_3
//		FROM Actor JOIN Movie AS m ON m.id=mid JOIN Person AS p ON pid=p.id
//		WHERE byear IS NOT NULL
//		AND rating IS NOT NULL
//		AND runtime IS NOT NULL
//		AND year BETWEEN 1990 AND 2010
//		AND totalvotes > 1000
//		AND mid IN (SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE '%c%')

		// TODO Your code here!
		PreparedStatement preparedStatement = con.prepareStatement(sqlQuery);
		preparedStatement.setFetchSize(1000);
		ResultSet resultSet = preparedStatement.executeQuery();
		ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
		int numberOfDimensions = resultSetMetaData.getColumnCount() - 1;
		ResultList resultList = new ResultList(resultSet, numberOfDimensions);
		int resultSize = resultList.self.size();
		ArrayList<ArrayList<Decimal128>> limits = new ArrayList<>(numberOfDimensions);
		for (int i = 0; i < numberOfDimensions; i++) {
			limits.add(new ArrayList<>(resultSize));
		}
		for (Map.Entry entry : resultList.self.entrySet()) {
			Document d = new Document();
			d.append("_id", "p_" + entry.getKey());
			Map<String, Object> dims = new HashMap<>();
			int count = 0;
			for (Decimal128 dimValue: (ArrayList<Decimal128>) entry.getValue()) {
				dims.put("dim_" + count, dimValue);
				limits.get(count).add(dimValue);
				count++;
			}
			d.append("point", new Document(dims));
			//collection.insertOne(d);
		}
		ArrayList<Limit> dimensionLimits = new ArrayList<>(numberOfDimensions);
		for (int i = 0; i < numberOfDimensions; i++) {
			dimensionLimits.add(new Limit(limits.get(i)));
		}
		System.out.println(dimensionLimits);
		/*
		 * 
		 * The SQL query has a column named id with the id of each point (always long), and a number of columns dim_i that form the point with n dimensions. 
		 * 	You should store the value of each dimension as a Decimal128. In order to do so, use the readAttribute method provided.
		 * 
		 * All your computations must use BigDecimal/Decimal128. Note that x.add(y), where both x and y are BigDecimal, will not update x, so you need to 
		 * 	assign it to a BigDecimal, i.e., z = x.add(y). When dividing, use MathContext.DECIMAL128 to keep the desired precision. If you implement your 
		 * 	calculations using MongoDB, do not use {$divide : [x, y]}; instead, you must do: {$multiply : [x, {$pow: [y, -1]}]}.
		 * 
		 * Each point must be of the form: {_id: p_123, point: {dim_0:_, dim_1:_, ...}}; each centroid: {_id: c_7, centroid: {}}.
		 * 
		 * Compute stat values per dimension and store them in a document whose id is 'limits'. For each dimension i, dim_i:{min:_, max:_, mean:_, std:_}.
		 * 	Note that you can use Java or MongoDB to compute these. There is a stdDevPop in MongoDB to compute standard deviation; unfortunately, it does
		 * 	not return Decimal128, so you need to find an alternate way.
		 * 
		 * Using the limits, you must scale the value using MinMax, Mean or ZScore according to the input. This only applies to the points.
		 * 
		 */
		
			
		
		// TODO End of your code!
		
		client.close();
		con.close();
	}
	
	private static Decimal128 readAttribute(ResultSet rs, String label) throws SQLException {
		// From: https://stackoverflow.com/questions/9482889/set-specific-precision-of-a-bigdecimal
		BigDecimal x = rs.getBigDecimal(label);
		x = x.setScale(x.scale() + MathContext.DECIMAL128.getPrecision() - x.precision(), MathContext.DECIMAL128.getRoundingMode());
		return new Decimal128(x);
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

class ResultList {
	HashMap<Long, ArrayList<Decimal128>> self;

	public ResultList(ResultSet rs, int numberOfDimensions) throws SQLException {
		this.self = new HashMap<Long, ArrayList<Decimal128>>();
		while (rs.next()) {
			long id = rs.getLong("id");
			if (!this.self.containsKey(id)) {
				this.self.put(id, new ArrayList<>(numberOfDimensions));
			}
			ArrayList<Decimal128> tempDims = this.self.get(id);
			for (int i = 0; i < numberOfDimensions; i++) {
				BigDecimal x = rs.getBigDecimal("dim_" + i);
				x = x.setScale(x.scale() + MathContext.DECIMAL128.getPrecision() - x.precision(), MathContext.DECIMAL128.getRoundingMode());
				tempDims.add(new Decimal128(x));
			}
		}
	}

	@Override
	public String toString() {
		return "ResultList{" +
				"self=" + self.size() +
				'}';
	}
}

class Limit {
	Decimal128 min;
	Decimal128 max;
	BigDecimal mean;
	Decimal128 std;
	public Limit(ArrayList<Decimal128> vals) {
		this.min = Collections.min(vals);
		this.max = Collections.max(vals);
		BigDecimal sum = new BigDecimal(0);
		sum = sum.setScale(sum.scale() + MathContext.DECIMAL128.getPrecision() - sum.precision(), MathContext.DECIMAL128.getRoundingMode());
		for (int i = 0; i < vals.size(); i++) {
			sum = sum.add(vals.get(i).bigDecimalValue());
		}
		BigDecimal mean = sum.divide(BigDecimal.valueOf(vals.size()), 31, MathContext.DECIMAL128.getRoundingMode());
		this.mean = mean.setScale(31, MathContext.DECIMAL128.getRoundingMode());
	}

	@Override
	public String toString() {
		return "Limit{" +
				"min=" + min +
				", max=" + max +
				", mean=" + mean +
				", std=" + std +
				"}\n";
	}
}
