package edu.rit.ibd.a6;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class AprioriGenOpt_Template {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColLKMinusOne = args[2];
		final String mongoColLK = args[3];
		final int minSup = Integer.valueOf(args[4]);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> lKMinusOne = db.getCollection(mongoColLKMinusOne);
		MongoCollection<Document> lk = db.getCollection(mongoColLK);
		
		// TODO Your code here!
		
		/*
		 * 
		 * The documents include the transactions that contain them, so if a new document is added to CK, we can directly compute its transactions by performing the intersection. Having the actual
		 * 	transactions entails that we also know its number, so we can discard those that do not meet the minimum support. Items can be processed in ascending order.
		 * 
		 */
		
		// You must figure out the value of k - 1.
		
		// You can implement this "by hand" using Java, an aggregation query, or a mix.
		
		// Remember that there is a single join step. The prune step is not used anymore.
		
		// Make sure the _ids of the documents are according to the lexicographical order of the items. You can start joining documents
		//	whose _ids are strictly greater than the current document. Also, the first time a pair of documents do not join, we can safely stop.
		
		// Both documents contain the arrays of transactions lexicographically sorted. The new document will have the intersecion of both sets
		//	of transactions.
		
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
