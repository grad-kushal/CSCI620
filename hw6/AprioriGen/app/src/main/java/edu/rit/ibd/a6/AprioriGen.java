package edu.rit.ibd.a6;

import com.mongodb.client.FindIterable;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import javax.print.Doc;

public class AprioriGen {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColLKMinusOne = args[2];
		final String mongoColCK = args[3];
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> lKMinusOne = db.getCollection(mongoColLKMinusOne);
		MongoCollection<Document> ck = db.getCollection(mongoColCK);

		// TODO Your code here!
		Document dTemp = lKMinusOne.find().first();
		int kMinusOne = ((Document)dTemp.get("items")).size();
		FindIterable<Document> iterableCollection = lKMinusOne.find().batchSize(101);
		for (Document d1 : iterableCollection) {
			for (Document d2 : iterableCollection) {
				if (!d1.equals(d2)) {
					Document d1Items = (Document) d1.get("items");
					Document d2Items = (Document) d2.get("items");
					for (int i = 0; i < kMinusOne; i++) {
						boolean flag = true;
						if (! (d1Items.get("pos_" + i) == d2Items.get("pos_" + i))) {
							flag = false;
							break;
						}
						if (flag == false) {

						}
					}
				}
			}
		}
		/*
		 * 
		 * First, you must figure out the current k-1 by checking the number of items in the input collection.
		 * 
		 * Then, you must start two pointers p and q such that p.pos_0==q.pos_0 AND p.pos_1==q.pos_1 AND ... AND p.pos_k-2==q.pos_k-2. Furthermore,
		 * 	p.pos_k-1<q.pos_k-1.
		 * 
		 * If the previous condition is true, a new document d as follows is candidate to be added:
		 * 		p.pos_0, p.pos_1, ... p.pos_k-2, p.pos_k-1, q.pos_k-1
		 * 
		 * Before adding it, we must check that all its subsets of size (k-1) were present in Lk-1. Use Sets.combinations to get each of these subsets. 
		 * 	If for a given subset s, there is no document that contains s, the previous document d is pruned.
		 * 
		 * Otherwise, d is added to Ck.
		 * 
		 */
		
		// You must figure out the value of k - 1.
		
		// You can implement this "by hand" using Java, an aggregation query, or a mix.
		
		// Remember that there is the join and the prune steps. Use Sets.combinations for the prune step.
		// Skip the prune step for L1.
		
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
