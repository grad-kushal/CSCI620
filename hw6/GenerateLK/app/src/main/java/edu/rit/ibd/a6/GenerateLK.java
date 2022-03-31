package edu.rit.ibd.a6;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;

public class GenerateLK {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColTrans = args[2];
		final String mongoColCK = args[3];
		final String mongoColLK = args[4];
		final int minSup = Integer.valueOf(args[5]);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> transactions = db.getCollection(mongoColTrans);
		MongoCollection<Document> ck = db.getCollection(mongoColCK);
		MongoCollection<Document> lk = db.getCollection(mongoColLK);
		UpdateOptions options = new UpdateOptions().upsert(true);
		
		// TODO Your code here!
		for (Document t : transactions.find().batchSize(101)) {
			for (Document c : ck.find().batchSize(101)) {
				ArrayList<Integer> transactionItems = (ArrayList<Integer>)t.get("items");
				Document cItemsDocument = (Document)c.get("items");
				int cCount = c.getInteger("count");
				//System.out.println("INITIAL COUNT: " + cCount);
				ArrayList<Integer> cItemsList = new ArrayList<>();
				int k = cItemsDocument.size();
				for (int i = 0; i < k; i++) {
					cItemsList.add(cItemsDocument.getInteger("pos_" + i));
				}
				if (transactionItems.containsAll(cItemsList)){
					cCount++;
					Bson updates = Updates.combine(Updates.inc("count", 1));
					ck.updateOne(c, updates, options);
				}
			}
		}
		for (Document d : ck.aggregate(Arrays.asList(
				Aggregates.match(Filters.gte("count", minSup))
		)).batchSize(101)) {
			lk.insertOne(d);
		}
		/*
		 * 
		 * For each transaction t, check whether the items of a document c in ck are contained in the items of t. If so, increment by one the count of c.
		 * 
		 * All the documents in ck that meet the minimum support will be copied to lk.
		 * 
		 * You can use $inc to update the count of a document.
		 * 
		 * Alternatively, you can also copy all documents in ck to lk first and, then, perform the previous computations.
		 * 
		 */
		
		// You must figure out the value of k.
		
		// For each document in Ck, check the items are present in the transactions at least minSup times.
		
		// You can implement this "by hand" using Java, an aggregation query, or a mix.
		
		// TODO End of your code here!
		
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
