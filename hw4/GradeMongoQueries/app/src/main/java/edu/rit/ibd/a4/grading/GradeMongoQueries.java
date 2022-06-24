package edu.rit.ibd.a4.grading;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.Document;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class GradeMongoQueries {
	private static final Map<Integer, List<Document>> expectedData = new HashMap<>();

	static {
		expectedData.put(1, Lists.newArrayList(Document.parse("{_id: 2187052}"), Document.parse("{_id: 2360424}"),
				Document.parse("{_id: 2461190}"), Document.parse("{_id: 2560988}"), Document.parse("{_id: 2739140}"),
				Document.parse("{_id: 2882232}"), Document.parse("{_id: 3067774}"), Document.parse("{_id: 3203962}"),
				Document.parse("{_id: 3295964}"), Document.parse("{_id: 3428892}"), Document.parse("{_id: 3815748}"),
				Document.parse("{_id: 4050462}"), Document.parse("{_id: 4054382}"), Document.parse("{_id: 4331676}"),
				Document.parse("{_id: 4449798}"), Document.parse("{_id: 4803104}"), Document.parse("{_id: 4879102}"),
				Document.parse("{_id: 5001140}"), Document.parse("{_id: 5091616}"), Document.parse("{_id: 5467530}"),
				Document.parse("{_id: 5731424}"), Document.parse("{_id: 6325386}"), Document.parse("{_id: 6330742}"),
				Document.parse("{_id: 6571050}"), Document.parse("{_id: 6737634}"), Document.parse("{_id: 7579702}"),
				Document.parse("{_id: 8442226}"), Document.parse("{_id: 9015842}"), Document.parse("{_id: 9624686}"),
				Document.parse("{_id: 10885578}"), Document.parse("{_id: 11263418}"), Document.parse("{_id: 11647116}"),
				Document.parse("{_id: 11847126}"), Document.parse("{_id: 11930146}"),
				Document.parse("{_id: 13204756}")));
		expectedData.put(2, Lists.newArrayList(Document.parse("{_id: 332220}"), Document.parse("{_id: 9034308}"),
				Document.parse("{_id: 316290}"), Document.parse("{_id: 158628}"), Document.parse("{_id: 463935}"),
				Document.parse("{_id: 331508}"), Document.parse("{_id: 3224036}"), Document.parse("{_id: 4050462}"),
				Document.parse("{_id: 3165264}"), Document.parse("{_id: 114558}"), Document.parse("{_id: 113799}"),
				Document.parse("{_id: 144814}"), Document.parse("{_id: 103178}")));
		expectedData.put(3, Lists.newArrayList(Document.parse("{_id: 7131}"), Document.parse("{_id: 311508}"),
				Document.parse("{_id: 247}"), Document.parse("{_id: 403226}"), Document.parse("{_id: 1353}"),
				Document.parse("{_id: 399065}"), Document.parse("{_id: 939147}"), Document.parse("{_id: 628838}"),
				Document.parse("{_id: 498080}"), Document.parse("{_id: 2260417}"), Document.parse("{_id: 443070}"),
				Document.parse("{_id: 4630}"), Document.parse("{_id: 150906}"), Document.parse("{_id: 35272}"),
				Document.parse("{_id: 1531337}"), Document.parse("{_id: 553941}"), Document.parse("{_id: 620609}"),
				Document.parse("{_id: 7139}")));
		expectedData.put(4, Lists.newArrayList(
				Document.parse("{_id: 12731980, rating: {$numberDecimal: '7.3000000000000000000000000000000'}}"),
				Document.parse("{_id: 10696784, rating: {$numberDecimal: '6.6000000000000000000000000000000'}}")));
		expectedData.put(5, Lists.newArrayList(Document.parse("{_id: {byear: 1940}, people: [40297]}"),
				Document.parse("{_id: {byear: 1924}, people: [688467]}"),
				Document.parse("{_id: {byear: 1968}, people: [411126]}"),
				Document.parse("{_id: {byear: 1971}, people: [475525]}"),
				Document.parse("{_id: {byear: 1952}, people: [356]}"),
				Document.parse("{_id: {byear: 1918, dyear: 1978}, people: [768032]}"),
				Document.parse("{_id: {byear: 1997}, people: [4679580, 2401904]}"),
				Document.parse("{_id: {byear: 1936, dyear: 2018}, people: [622416]}"),
				Document.parse("{_id: {byear: 1985}, people: [3844249]}"),
				Document.parse("{_id: {byear: 1998}, people: [5140138]}"),
				Document.parse("{_id: {byear: 1949}, people: [605717, 847491]}"),
				Document.parse("{_id: {byear: 1951}, people: [607636, 84196, 849006, 244740]}"),
				Document.parse("{_id: {byear: 1924, dyear: 2005}, people: [447230]}"),
				Document.parse("{_id: {byear: 1976}, people: [2554232]}"),
				Document.parse("{_id: {byear: 1927}, people: [649851, 840646]}"),
				Document.parse("{_id: {}, people: [109327, 3993587, 3196483, 526541, 1645226, 2457309, 4962340, 7630887, "
						+ "4559884, 98567, 8532778, 2880219, 298255, 6892993, 7401698, 4920430, 8908425, 5093888, 3777345, 5381103, "
						+ "4530068, 3741147, 1250821, 312040, 3427158, 1993800, 6508261, 129203, 5869319, 6612832, 5273629, 8296800, "
						+ "1150734, 3542918, 188173, 7177352, 447896, 9184356, 7333844, 3095533, 3877583, 690307, 3635216, 256629, "
						+ "3986921, 5511218, 7376220, 442892, 1377113, 913314, 4143428, 10513491, 670372, 1467957, 2825577, 915026, "
						+ "4762616, 4876031, 5006702, 8421253, 6045344, 4086158, 10245341, 7180930, 3609242, 4398223, 5929034, "
						+ "7333837, 8783705, 154240, 6765770, 492169, 915933]}"),
				Document.parse("{_id: {byear: 1922, dyear: 2016}, people: [58117]}"),
				Document.parse("{_id: {byear: 1932, dyear: 1998}, people: [442708]}"),
				Document.parse("{_id: {byear: 1965}, people: [499614, 847392, 768005]}"),
				Document.parse("{_id: {byear: 1958}, people: [919297]}"),
				Document.parse("{_id: {byear: 1982}, people: [3426050]}"),
				Document.parse("{_id: {byear: 2005}, people: [8630118]}"),
				Document.parse("{_id: {byear: 1987}, people: [5156521]}"),
				Document.parse("{_id: {byear: 1910, dyear: 1986}, people: [1079]}"),
				Document.parse("{_id: {byear: 1980}, people: [2366218, 950778]}"),
				Document.parse("{_id: {byear: 1957}, people: [1146]}"),
				Document.parse("{_id: {byear: 1972}, people: [643894]}"),
				Document.parse("{_id: {byear: 1945}, people: [674221]}"),
				Document.parse("{_id: {byear: 1952, dyear: 1983}, people: [645625]}"),
				Document.parse("{_id: {byear: 1903, dyear: 1992}, people: [110480]}"),
				Document.parse("{_id: {byear: 1938, dyear: 2017}, people: [171824]}"),
				Document.parse("{_id: {byear: 1959}, people: [192947]}"),
				Document.parse("{_id: {byear: 1973}, people: [5503026, 693925, 3750817]}"),
				Document.parse("{_id: {byear: 1948}, people: [24350]}"),
				Document.parse("{_id: {byear: 1960}, people: [38358]}"),
				Document.parse("{_id: {byear: 1929, dyear: 1985}, people: [906745]}"),
				Document.parse("{_id: {byear: 1950}, people: [484]}"),
				Document.parse("{_id: {byear: 1923, dyear: 2010}, people: [462013]}"),
				Document.parse("{_id: {byear: 1947}, people: [766368]}"),
				Document.parse("{_id: {byear: 1938}, people: [614030]}"),
				Document.parse("{_id: {byear: 1929}, people: [596384]}")));
		expectedData.put(6, Lists.newArrayList(Document.parse("{_id: 530865}"), Document.parse("{_id: 939147}")));
	}

	public static void main(String[] args) {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String folderToAssignment = args[2];

		// Name of the folder containing the SQL queries.
		final String folderToSearch = "Mongo";
		// Maximum size in MB of the Gradle project without build.
		final double maxSize = 2048.0;

		MongoClient client = null;
		MongoDatabase db = null;

		// Get each of the folders in the directory. Each folder is a student's
		// username, e.g., crrvcs.
		for (File studentFolder : new File(folderToAssignment).listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.isDirectory();
			}
		})) {

			if (studentFolder.getName().startsWith("_"))
				continue;

			System.out.println("Student: " + studentFolder.getName());
			try {
				// Search for the folder no further than three steps.
				File sqlProject = searchFolder(studentFolder, folderToSearch, 3);

				if (sqlProject != null) {
					// Get total size and make sure it is within the requirements.
					long size = getFolderSize(sqlProject);
					if (size / 1024.0 > maxSize) {
						System.out.println("The total size is " + size / 1024.0 + " which is larger than expected.");
						continue;
					}

					client = getClient(mongoDBURL);
					db = client.getDatabase(mongoDBName);
					for (int i = 1; i <= 6; i++)
						try {
							// Read Mongo query.
							String queryStr = Files
									.readAllLines(Paths.get(new File(sqlProject, "Q" + i + ".json").toURI())).stream()
									.collect(Collectors.joining(" "));

							Document queryDoc = Document.parse(queryStr);

							MongoCollection<Document> col = db.getCollection(queryDoc.getString("initialCollection"));

							if (col == null) {
								System.out.println("The collection " + queryDoc.getString("initialCollection")
										+ " does not exist");
								System.out.println("\tPenalty: -7");
								continue;
							}

							int timeLimit = 25;

							// Run query.
							List<Document> retrieved = new ArrayList<>();
							long before = System.nanoTime();
							for (Document doc : col.aggregate(queryDoc.getList("pipeline", Document.class))
									.maxTime(timeLimit, TimeUnit.SECONDS))
								retrieved.add(doc);
							long after = System.nanoTime();

							System.out.println("Query " + i + " took " + ((after - before) / 1e9) + " secs");

							// Copy expected.
							List<Document> expected = new ArrayList<>();
							Set<String> expectedFields = new HashSet<>();
							for (Document doc : expectedData.get(i)) {
								Document newD = new Document(doc);
								listsToSets(newD);
								expected.add(newD);
								expectedFields.addAll(doc.keySet());
							}

							boolean issuesWithQuery = false;
							// Read result.
							for (Document doc : retrieved) {
								Set<String> docKeys = new HashSet<>(doc.keySet());
								for (String key : docKeys)
									if (!expectedFields.contains(key))
										doc.remove(key);
								listsToSets(doc);

								// Find tuple in expected.
								Integer found = null;
								for (int j = 0; found == null && j < expected.size(); j++)
									if (Maps.difference(doc, expected.get(j)).areEqual())
										found = j;

								if (found != null)
									expected.remove(found.intValue());
								else {
									System.out.println("\tDocument " + doc.toJson() + " not expected");
									issuesWithQuery = true;
								}
							}

							if (!expected.isEmpty())
								for (Document doc : expected) {
									System.out.println("\tExpected document " + doc.toJson() + " not found");
									issuesWithQuery = true;
								}

							if (issuesWithQuery)
								System.out.println("\tPenalty: -5");
						} catch (Exception oops) {
							System.out.println(
									"There was a serious issue with Query " + i + " (" + oops.getMessage() + ")");
							System.out.println("\tPenalty: -7");
						}

					// Close connection.
					client.close();
				} else
					System.out.println("Folder not found!");
			} catch (Throwable oops) {
				System.out.println("Something went really wrong!");
				oops.printStackTrace();
			}

			System.out.println();
			System.out.println();
		}

	}

	private static void listsToSets(Document d) {
		for (String key : d.keySet())
			try {
				List<Object> list = d.getList(key, Object.class);
				d.put(key, new HashSet<>(list));
			} catch (Exception oops) {
				/* Do nothing. */}
	}

	private static MongoClient getClient(String mongoDBURL) {
		MongoClient client = null;
		if (mongoDBURL.equals("None"))
			client = new MongoClient();
		else
			client = new MongoClient(new MongoClientURI(mongoDBURL));
		return client;
	}

	// In bytes.
	public static long getFolderSize(File folder) throws Exception {
		AtomicLong size = new AtomicLong();
		Files.walk(folder.toPath()).forEach(f -> {
			File file = f.toFile();
			if (file.isFile()) {
				size.addAndGet(file.length());
			}
		});
		return size.get();
	}

	public static File searchFolder(File folder, String nameToSearch, int depth) throws Exception {
		List<Path> result;
		try (Stream<Path> pathStream = Files.find(folder.toPath(), depth,
				(p, basicFileAttributes) -> p.toFile().isDirectory() && p.toFile().getName().equals(nameToSearch))) {
			result = pathStream.collect(Collectors.toList());
		}
		if (result.size() > 1)
			throw new Error("Several folders found!");
		else if (result.isEmpty())
			return null;
		else
			return result.get(0).toFile();
	}

}
