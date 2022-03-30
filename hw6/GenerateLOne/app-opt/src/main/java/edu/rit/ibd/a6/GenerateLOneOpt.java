package edu.rit.ibd.a6;

import com.mongodb.client.model.CountOptions;
import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.HashMap;

public class GenerateLOneOpt {

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

		System.out.println(transactions);

		HashMap<Integer, Integer> itemFrequency = new HashMap<>();
		
		// TODO Your code here!

		for (Document d : transactions.find()) {
			System.out.println("Here@@@@@@@@@@@@@@@@@@@@@@@@");
			int tid = d.getInteger("tid");
			ArrayList<Integer> items = (ArrayList<Integer>) d.get("iid");
			//int iid = d.getInteger("iid");
			System.out.println("FFFFFF" + items);
		}
		/*
		 * 
		 * Extract single items from the transactions. Only single items that are present in at least minSup transactions should survive.
		 * 
		 * Keep track of the transactions associated to each item using an array field named 'transactions'. Also, use _ids such that
		 * 	they reflect the lexicographical order in which documents are processed.
		 * 
		 */
		
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
