package edu.rit.ibd.a4;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import com.mongodb.client.FindIterable;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import javax.print.Doc;

public class IMDBSQLToMongo {

	public static void main(String[] args) throws Exception {
		final String dbURL = args[0];
		final String user = args[1];
		final String pwd = args[2];
		final String mongoDBURL = args[3];
		final String mongoDBName = args[4];
		
		System.out.println(new Date() + " -- Started");
		
		Connection con = DriverManager.getConnection(dbURL, user, pwd);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		// TODO 0: Your code here!
		
		/*
		 * 
		 * Everything in MongoDB is a document (both data and queries). To create a document, I use primarily two options but there are others
		 * 	if you ask the Internet. You can use org.bson.Document as follows:
		 * 
		 * 		Document d = new Document();
		 * 		d.append("name_of_the_field", value);
		 * 
		 * 	The type of the field will be the conversion of the Java type of the value.
		 * 
		 * 	Another option is to parse a string representing the document:
		 * 
		 * 		Document d = Document.parse("{ _id:1, name:\"Name\" }");
		 * 
		 * 	It will parse only well-formed documents. Note that the previous approach will use the Java data types as the types of the pieces of
		 * 		data to insert in MongoDB. However, the latter approach will not have that info as everything is a string; therefore, be mindful
		 * 		of these differences and use the approach it will fit better for you.
		 * 
		 * If you wish to create an embedded document, you can use the following:
		 * 
		 * 		Document outer = new Document();
		 * 		Document inner = new Document();
		 * 		outer.append("doc", inner);
		 * 
		 * To connect to a MongoDB database server, use the getClient method above. If your server is local, just provide "None" as input.
		 * 
		 * You must extract data from MySQL and load it into MongoDB. Note that, in general, the data in MongoDB is denormalized, which means that it includes
		 * 	redundancy. You must think of ways of extracting such redundant data in batches, that is, you should think of a bunch of queries that will retrieve
		 * 	the whole database in a format it will be convenient for you to load in MongoDB. Performing many small SQL queries will not work.
		 * 
		 * If you execute a SQL query that retrieves large amounts of data, all data will be retrieved at once and stored in main memory. To avoid such behavior,
		 * 	the JDBC URL will have the following parameter: 'useCursorFetch=true' (already added by the grading software). Then, you can control the number of 
		 * 	tuples that will be retrieved and stored in memory as follows:
		 * 
		 * 		PreparedStatement st = con.prepareStatement("SELECT ...");
		 * 		st.setFetchSize(batchSize);
		 * 
		 * where batchSize is the number of rows.
		 * 
		 * Null values in MySQL must be translated as documents without such fields.
		 * 
		 * Once you have composed a specific document with the data retrieved from MySQL, insert the document into the appropriate collection as follows:
		 * 
		 * 		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);
		 * 
		 * 		...
		 * 
		 * 		Document d = ...
		 * 
		 * 		...
		 * 
		 * 		col.insertOne(d);
		 * 
		 * You should focus first on inserting all the documents you need (movies and people). Once those documents are already present, you should deal with
		 * 	the mapping relations. To do so, MongoDB is optimized to make small updates of documents referenced by their keys (different than MySQL). As a 
		 * 	result, it is a good idea to update one document at a time as follows:
		 * 
		 * 		PreparedStatement st = con.prepareStatement("SELECT ..."); // Select from mapping table.
		 * 		st.setFetchSize(batchSize);
		 * 		ResultSet rs = st.executeQuery();
		 * 		while (rs.next()) {
		 * 			col.updateOne(Document.parse("{ _id : "+rs.get(...)+" }"), Document.parse(...));
		 * 			...
		 * 
		 * The updateOne method updates one single document based on the filter criterion established in the first document (the _id of the document to fetch
		 * 	in this case). The second document provided as input is the update operation to perform. There are several updates operations you can perform (see
		 * 	https://docs.mongodb.com/v3.6/reference/operator/update/). If you wish to update arrays, $push and $addToSet are the best options but have slightly
		 * 	different semantics. Make sure you read and understand the differences between them.
		 * 
		 * When dealing with arrays, another option instead of updating one by one is gathering all values for a specific document and perform a single update.
		 * 
		 * Note that array fields that are empty are not allowed, so you should not generate them.
		 *  
		 */
		
		
		//MongoCollection<Document> col = db.getCollection("Collection");

		// Try to use few queries that retrieve big chunks of data rather than many queries that retrieve small pieces of data.
//		PreparedStatement st = con.prepareStatement("SELECT * from Movie");
//		st.setFetchSize(/* Batch size */ 10000);
//		ResultSet rs = st.executeQuery();
//		MovieTable movieTable = new MovieTable(rs);
//		//System.out.println(movieTable.movies.size());
//		MongoCollection<Document> col = db.getCollection("movie");
//		for (Movie m : movieTable.movies) {
//			Document d = new Document();
//			d.append("_id", m.id);
//			d.append("ptitle", m.ptitle);
//			d.append("otitle", m.otitle);
//			d.append("adult", m.adult);
//			if (m.year != 0) {
//				d.append("year", m.year);
//			}
//			if (m.runtime != 0) {
//				d.append("runtime", m.runtime);
//			}
//			if (m.rating != null) {
//				d.append("rating", new Decimal128(m.rating));
//			}
//			if (m.totalvotes != 0) {
//				d.append("totalvotes", m.totalvotes);
//			}
//			col.insertOne(d);
//		}
//		rs.close();
//		st.close();
//
//		st = con.prepareStatement("SELECT * from Person");
//		st.setFetchSize(/* Batch size */ 20000);
//		rs = st.executeQuery();
//		PersonTable personTable = new PersonTable(rs);
//		MongoCollection<Document> col2 = db.getCollection("person");
//		int cnt = 0;
//		for (Person p : personTable.people) {
//			//System.out.println(cnt++);
//			Document d = new Document();
//			d.append("_id", p.id);
//			d.append("name", p.name);
//			if (p.birthYear != 0)
//				d.append("byear", p.birthYear);
//			if (p.deathYear != 0)
//				d.append("dyear", p.deathYear);
//			col2.insertOne(d);
//		}
//		rs.close();
//		st.close();
//
//
//		st = con.prepareStatement("...");
		//st.setFetchSize(batchSize);
//		rs = st.executeQuery();
//		while (rs.next())
//			col.updateOne(/* Filter to grab a single document */ (Bson) null, /* Changes to perform; use $push/$addToSet to add values to arrays. */ (Bson) null);
//		rs.close();
//		st.close();

		PreparedStatement pr = con.prepareStatement("SELECT pid, mid FROM imdb_ibd_a1.person JOIN actor ON id=pid JOIN movie on movie.id = mid WHERE mid IN (select distinct mid from actor);")

		//Creating a collection object
		//MongoCollection<Document> collectionRead = db.getCollection("movie");
		//Retrieving the documents
		//FindIterable<Document> iterableDocument = collectionRead.find();
		//Iterator it = iterableDocument.iterator();
//		while (it.hasNext()) {
//			Document tempDoc = (Document) it.next();
//
//			System.out.println("GGGGGGGGGG: " + it.next().toString());
//			break;
//		}
		
		// TODO 0: End of your code.
		
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

class MovieTable {
	ArrayList<Movie> movies;

	public MovieTable(ResultSet rs) throws SQLException {
		this.movies = new ArrayList<>();
		while (rs.next()) {
			Movie m = new Movie(rs.getInt("id"),
					rs.getString("ptitle"),
					rs.getString("otitle"),
					rs.getBoolean("adult"),
					rs.getInt("year"),
					rs.getInt("runtime"),
					rs.getBigDecimal("rating"),
					rs.getInt("totalvotes"));
			movies.add(m);
		}
	}
}

class Movie {
	int id;
	String ptitle, otitle;
	boolean adult;
	int year;
	int runtime;
	BigDecimal rating;
	int totalvotes;

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}

		if (!(o instanceof Movie)) {
			return false;
		}

		Movie m = (Movie) o;

		return this.id == m.id;
	}

	public Movie(int id, String ptitle, String otitle, boolean adult, int year, int runtime, BigDecimal rating, int totalvotes) {
		this.id = id;
		this.ptitle = ptitle;
		this.otitle = otitle;
		this.adult = adult;
		this.year = year;
		this.runtime = runtime;
		this.rating = rating;
		this.totalvotes = totalvotes;
	}
}

class Person {
	int id;
	String name;
	int birthYear;
	int deathYear;

	public Person(int id, String name, int birthYear, int deathYear) {
		this.id = id;
		this.name = name;
		this.birthYear = birthYear;
		this.deathYear = deathYear;
	}
}

class PersonTable {
	ArrayList<Person> people;

	public PersonTable(ResultSet rs) throws SQLException {
		this.people = new ArrayList<>();
		while (rs.next()) {
			Person p = new Person(rs.getInt("id"),
					rs.getString("name"),
					rs.getInt("byear"),
					rs.getInt("dyear"));
			people.add(p);
		}
	}
}
