package edu.rit.ibd.a6;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class GenerateLOne {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColTrans = args[2];
		final String mongoColL1 = args[3];
		final int minSup = Integer.valueOf(args[4]);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> transactions = db.getCollection(mongoColTrans);
		MongoCollection<Document> l1 = db.getCollection(mongoColL1);

		HashMap<Integer, Integer> itemFrequency = new HashMap<Integer, Integer>();

		// TODO Your code here!

		//for (Document d : transactions.aggregate(Arrays.asList(Aggregates.match())))
		for (Document d : transactions.find().batchSize(101)) {
			int tid = d.getInteger("_id");
			ArrayList<Integer> items = (ArrayList<Integer>) d.get("items");
			for (Integer item: items) {
				if (!itemFrequency.containsKey(item)){
					itemFrequency.put(item, 0);
				}
				int currentFrequency = itemFrequency.get(item);
				itemFrequency.put(item, currentFrequency + 1);
			}
		}

//		MongoCursor<Document> cursor = transactions.find().iterator();
//		while (cursor.hasNext()) {
//			Document d = cursor.next();
//			int tid = d.getInteger("_id");
//			ArrayList<Integer> items = (ArrayList<Integer>) d.get("items");
//			for (Integer item: items) {
//				if (!itemFrequency.containsKey(item)){
//					itemFrequency.put(item, 0);
//				}
//				int currentFrequency = itemFrequency.get(item);
//				itemFrequency.put(item, currentFrequency + 1);
//			}
//		}
		for (Map.Entry entry : itemFrequency.entrySet()) {
			int fTemp = (int) entry.getValue();
			if (fTemp >= minSup) {
				Document d = new Document();
				d.append("count", fTemp);
				d.append("items", new Document().append("pos_0", entry.getKey()));
				l1.insertOne(d);
			}
		}
		
		/*
		 * 
		 * Extract single items from the transactions. Only single items that are present in at least minSup transactions should survive.
		 * 
		 * You need to compose the new documents to be inserted in the L1 collection as {_id: {pos_0:iid}, count:z}.
		 * 
		 */
		
		// You can implement this "by hand" using Java, an aggregation query, or a mix.
		// Be mindful of main memory and use batchSize when you request documents from MongoDB.
		
		// TODO End of your code!
		
		client.close();
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
