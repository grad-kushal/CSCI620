package edu.rit.ibd.a4.grading;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.Document;
import org.bson.types.Decimal128;

import com.google.common.collect.Lists;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class GradeIMDBSQLToMongo {
	private static final Map<Integer, Map<String, Object>> expectedMovieData = new HashMap<>(), expectedDenormMovieData = new HashMap<>(),
			expectedPersonData = new HashMap<>(), expectedDenormPersonData = new HashMap<>();
	private static final Map<Integer, Integer> expectedActor = new HashMap<>(), expectedDirector = new HashMap<>();
	private static final Map<String, Integer> expectedGenre = new HashMap<>();
	
	static {
		expectedMovieData.put(78748, new Document().append("_id", 78748).append("ptitle", "Alien").
				append("adult", false).append("year", 1979).append("runtime", 117).append("rating", new Decimal128(new BigDecimal("8.4000000000000000000000000000000"))).
					append("totalvotes", 837225).append("genres", Lists.newArrayList("Horror", "Sci-Fi")));
		expectedMovieData.put(88763, new Document().append("_id", 88763).append("ptitle", "Back to the Future").
				append("adult", false).append("year", 1985).append("runtime", 116).append("rating", new Decimal128(new BigDecimal("8.5000000000000000000000000000000"))).
				append("totalvotes", 1133998).append("genres", Lists.newArrayList("Adventure", "Comedy", "Sci-Fi")));
		
		expectedDenormMovieData.put(88763, new Document().append("_id", 88763).append("actors", 4).append("directors", 1).append("producers", 1).append("writers", 2));
		
		expectedPersonData.put(150, new Document().append("_id", 150).append("name", "Michael J. Fox").append("byear", 1961));
		expectedPersonData.put(244, new Document().append("_id", 244).append("name", "Sigourney Weaver").append("byear", 1949));
		expectedPersonData.put(709, new Document().append("_id", 709).append("name", "Robert Zemeckis").append("byear", 1951));
		expectedDenormPersonData.put(709, new Document().append("_id", 709).append("acted", 1).append("directed", 24).append("knownfor", 4).
				append("produced", 20).append("written", 14));
		expectedDenormPersonData.put(7, new Document().append("_id", 7).append("acted", 73).append("knownfor", 4));
		
		expectedActor.putAll(Map.of(78748, 1, 88763, 1));
		expectedDirector.putAll(Map.of(150, 0, 244, 0, 116, 16));
		expectedGenre.putAll(Map.of("Comedy", 101620, "Drama", 214822));
	}


	public static void main(String[] args) {
		final String jdbcUser = args[0];
		final String jdbcPwd = args[1];
		final String jdbcSchema = args[2];
		final String mongoDBURL = args[3];
		final String folderToAssignment = args[4];
		final boolean windows = Boolean.valueOf(args[5]);
		
		// Name of the folder containing the Gradle project.
		final String folderToSearch = "IMDBSQLToMongo";
		// Maximum size in MB of the Gradle project without build.
		final double maxSize = 2048.0;
		// Maximum amount of main memory allowed in MB.
		final int maxMem = 512;
		
		MongoClient client = null;
		MongoDatabase db = null;
		
		// Get each of the folders in the directory. Each folder is a student's username, e.g., crrvcs.
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
				File gradleProject = searchFolder(studentFolder, folderToSearch, 3);
				
				if (gradleProject != null) {
					File buildFolder = new File(gradleProject, "app/build");
					
					// Delete all previous builds first.
					if (buildFolder.exists())
						MoreFiles.deleteRecursively(Paths.get(buildFolder.toURI()), RecursiveDeleteOption.ALLOW_INSECURE);
					
					// Get total size and make sure it is within the requirements.
					long size = getFolderSize(gradleProject);
					if (size/1024.0 > maxSize) {
						System.out.println("The total size is " + size/1024.0 + " which is larger than expected.");
						continue;
					}
					
					// Build and install Gradle.
					File gradlewFile = new File(gradleProject.getAbsolutePath() + "/gradlew"+(windows?".bat":""));
					if (!windows)
						Files.setPosixFilePermissions(Paths.get(gradlewFile.toURI()), PosixFilePermissions.fromString("rwxrwxr-x"));
					
					boolean error = runProcess(new String[]{gradlewFile.getAbsolutePath(), "-p", gradleProject.getAbsolutePath(), "build"}, 5);
					error = error || runProcess(new String[]{gradlewFile.getAbsolutePath(), "-p", gradleProject.getAbsolutePath(), "installDist"}, 5);
					if (error) {
						System.out.println("Could not compile/install project.");
						continue;
					}
					
					// Get JDBC URL.
					String jdbcURL = "jdbc:mysql://localhost:3306/"+jdbcSchema+"?useCursorFetch=true",
							mongoDBName = studentFolder.getName() + "_IMDB";
					
					// Destroy all existing collections!
					client = getClient(mongoDBURL);
					db = client.getDatabase(mongoDBName);
					for (String col : db.listCollectionNames())
						db.getCollection(col).drop();
					client.close();
					
					// Let's run the program!
					ProcessBuilder builder = new ProcessBuilder(gradleProject.getAbsolutePath() + 
							"/app/build/install/app/bin/app" + (windows?".bat":""), jdbcURL, jdbcUser, jdbcPwd, mongoDBURL, mongoDBName).redirectErrorStream(true);
					builder.environment().put("JAVA_OPTS", "-Xmx" + maxMem + "m");
			
					Process process = null;
					try {
						long before = System.nanoTime();
						// Start the process.
						process = builder.start();
						// Change false/true to print to console the output of the program.
						StreamGobbler gobbler = new GradeIMDBSQLToMongo().new StreamGobbler(process.getInputStream(), false);
						// Start the gobbler to collect the console output if needed.
						gobbler.start();
						// We will wait a little bit...
						boolean done = process.waitFor(100, TimeUnit.MINUTES);
						long after = System.nanoTime();
						double timeTaken = (after - before) / (1e9 * 3600);
						System.out.println("The process took " + timeTaken + " hours; ");
						
						// Not done, penalty and destroy process.
						if (!done) {
							System.out.println("The process did not run in the expected time.");
							System.out.println("\tPenalty: -5");
							destroyProcess(process);
						}
						
						AtomicBoolean schemaIssues = new AtomicBoolean(false), 
								dataIssues = new AtomicBoolean(false);
						StringBuffer reasons = new StringBuffer();
						
						// Let's check the results.
						client = getClient(mongoDBURL);
						db = client.getDatabase(mongoDBName);
						
						MongoCollection<Document> movies = db.getCollection("Movies"), moviesDenorm = db.getCollection("MoviesDenorm"),
								people = db.getCollection("People"), peopleDenorm = db.getCollection("PeopleDenorm");
						
						if (movies == null || moviesDenorm == null || people == null || peopleDenorm == null) {
							reasons.append("Some of the expected collections did not exist; ");
							schemaIssues.set(true);
							dataIssues.set(true);
						} else {
							if (movies.countDocuments() != 598364l || moviesDenorm.countDocuments() != 598364l) {
								reasons.append("The movies collection(s) did not have the expected size; ");
								dataIssues.set(true);
							}
							
							if (people.countDocuments() != 11315414l || peopleDenorm.countDocuments() != 11315414l) {
								reasons.append("The people collection(s) did not have the expected size; ");
								dataIssues.set(true);
							}
							
							// Check there are no empty arrays.
							List<Document> q = new ArrayList<>();
							
							q.add(Document.parse("{ $match : { genres : { $size : 0 } } }"));
							q.add(Document.parse("{ $limit : 1 }"));
							if (movies.aggregate(q).first() != null || moviesDenorm.aggregate(q).first() != null) {
								reasons.append("There were empty genre arrays; ");
								dataIssues.set(true);
							}
							q.clear();
							
							for (String field : new String[] {"actors", "directors", "producers", "writers"}) {
								q.add(Document.parse("{ $match : { "+field+" : { $size : 0 } } }"));
								q.add(Document.parse("{ $limit : 1 }"));
								
								if (moviesDenorm.aggregate(q).first() != null) {
									reasons.append("There were empty "+field+"; ");
									dataIssues.set(true);
								}
								
								q.clear();
							}
							
							for (String field : new String[] {"acted", "directed", "knownfor", "produced", "written"}) {
								q.add(Document.parse("{ $match : { "+field+" : { $size : 0 } } }"));
								q.add(Document.parse("{ $limit : 1 }"));
								
								if (peopleDenorm.aggregate(q).first() != null) {
									reasons.append("There were empty "+field+"; ");
									dataIssues.set(true);
								}
								
								q.clear();
							}
							
							for (Integer movie : expectedMovieData.keySet()) {
								Document movieDoc = movies.find(Document.parse("{_id:"+movie+"}")).first(),
										movieDenormDoc = moviesDenorm.find(Document.parse("{_id:"+movie+"}")).first();
								
								if (movieDoc == null || movieDenormDoc == null) {
									reasons.append("Movie " + movie + " did not exist; ");
									dataIssues.set(true);
									continue;
								}
								
								if (movieDoc.containsKey("genres"))
									Collections.sort((List<String>) movieDoc.getList("genres", String.class));
								
								for (String attrib : expectedMovieData.get(movie).keySet())
									if (!movieDoc.containsKey(attrib)) {
										reasons.append("Mandatory attribute "+attrib+" not found in movie:"+movie+"; ");
										dataIssues.set(true);
									} else if (!movieDoc.get(attrib).equals(expectedMovieData.get(movie).get(attrib))) {
										reasons.append("Data for attribute "+attrib+" in movie:"+movie+" does not coincide: "+
												movieDoc.get(attrib)+"--"+expectedMovieData.get(movie).get(attrib)+"; ");
										dataIssues.set(true);
									}
							}
							
							for (Integer person : expectedPersonData.keySet()) {
								Document personDoc = people.find(Document.parse("{_id:"+person+"}")).first(),
										personDenormDoc = peopleDenorm.find(Document.parse("{_id:"+person+"}")).first();
								
								if (personDoc == null || personDenormDoc == null) {
									reasons.append("Person " + person + " did not exist; ");
									dataIssues.set(true);
									continue;
								}
								
								for (String attrib : expectedPersonData.get(person).keySet())
									if (!personDoc.containsKey(attrib)) {
										reasons.append("Mandatory attribute "+attrib+" not found in person:"+person+"; ");
										dataIssues.set(true);
									} else if (!personDoc.get(attrib).equals(expectedPersonData.get(person).get(attrib))) {
										reasons.append("Data for attribute "+attrib+" in person:"+person+"does not coincide: "+
												personDoc.get(attrib)+"--"+expectedPersonData.get(person).get(attrib)+"; ");
										dataIssues.set(true);
									}
							}
							
							for (Integer person : expectedDenormPersonData.keySet()) {
								Document personDenormDoc = peopleDenorm.find(Document.parse("{_id:"+person+"}")).first();
								
								if (personDenormDoc == null) {
									reasons.append("Person " + person + " in denormalized collection did not exist; ");
									dataIssues.set(true);
									continue;
								}
								
								personDenormDoc.remove("_id");
								
								for (String attrib : expectedDenormPersonData.get(person).keySet()) {
									if (attrib.equals("_id"))
										continue;
									
									if (!personDenormDoc.containsKey(attrib)) {
										reasons.append("Mandatory attribute "+attrib+" in denormalized person:"+person+" not found; ");
										dataIssues.set(true);
									} else {
										List<Integer> list = personDenormDoc.getList(attrib, Integer.class);
										int expectedSize = (int) expectedDenormPersonData.get(person).get(attrib);
										
										if ((list == null && expectedSize != 0) || (list != null && list.size() != expectedSize)) {
											reasons.append("Data for attribute "+attrib+" in denormalized person:"+person+" does not coincide: "+
												list==null?0:list.size()+"--"+expectedSize+"; ");
											dataIssues.set(true);
										}
									}
								}
							}
							
							for (Integer movie : expectedDenormMovieData.keySet()) {
								Document movieDenormDoc = moviesDenorm.find(Document.parse("{_id:"+movie+"}")).first();
								
								if (movieDenormDoc == null) {
									reasons.append("Movie " + movie + " in denormalized collection did not exist; ");
									dataIssues.set(true);
									continue;
								}
								
								movieDenormDoc.remove("_id");
								
								for (String attrib : expectedDenormMovieData.get(movie).keySet()) {
									if (attrib.equals("_id"))
										continue;
									
									if (!movieDenormDoc.containsKey(attrib)) {
										reasons.append("Mandatory attribute "+attrib+" in denormalized movie:"+movie+" not found; ");
										dataIssues.set(true);
									} else if (movieDenormDoc.getList(attrib, Integer.class).size() != (int) expectedDenormMovieData.get(movie).get(attrib)) {
										reasons.append("Data for attribute "+attrib+" in denormalized movie:"+movie+" does not coincide: "+
												movieDenormDoc.get(attrib)+"--"+expectedDenormMovieData.get(movie).get(attrib)+"; ");
										dataIssues.set(true);
									}
								}
							}
							
							for (Integer actor : expectedActor.keySet()) {
								Document actorDenormDoc = peopleDenorm.find(Document.parse("{_id:"+actor+"}")).first();
								
								q.add(Document.parse("{ $match : { _id : "+actor+" } }"));
								q.add(Document.parse("{ $lookup : { from : 'MoviesDenorm', localField : '_id', foreignField : 'actors', as : 'acted' } }"));
								q.add(Document.parse("{ $unwind : '$acted' }"));
								q.add(Document.parse("{ $group : { _id : '$_id', total : { $sum : 1 } } }"));
								Document actorDoc = people.aggregate(q).first();
								q.clear();
								
								if (actorDoc == null || actorDenormDoc == null) {
									reasons.append("Actor " + actor + " did not exist; ");
									dataIssues.set(true);
									continue;
								}
								
								if (((List<Integer>) actorDenormDoc.getList("acted", Integer.class)).size() != expectedActor.get(actor) || 
										actorDoc.getInteger("total") != expectedActor.get(actor)) {
									reasons.append("The total number of acted movies for actor "+actor+
											" was not as expected:"+((List<Integer>) actorDenormDoc.getList("acted", Integer.class)).size()+"; ");
									dataIssues.set(true);
								}
							}
							
							for (Integer director : expectedDirector.keySet()) {
								Document directorDenormDoc = peopleDenorm.find(Document.parse("{_id:"+director+"}")).first();
								
								q.add(Document.parse("{ $match : { _id : "+director+" } }"));
								q.add(Document.parse("{ $lookup : { from : 'MoviesDenorm', localField : '_id', foreignField : 'directors', as : 'directed' } }"));
								q.add(Document.parse("{ $unwind : '$directed' }"));
								q.add(Document.parse("{ $group : { _id : '$_id', total : { $sum : 1 } } }"));
								Document directorDoc = people.aggregate(q).first();
								q.clear();
								
								if ((expectedDirector.get(director) != 0 && directorDoc == null) || directorDenormDoc == null) {
									reasons.append("Director " + director + " did not exist; ");
									dataIssues.set(true);
									continue;
								}
								
								int directedFromDB = 0, directedFromAgg = 0;
								if (directorDenormDoc.containsKey("directed"))
									directedFromDB = ((List<Integer>) directorDenormDoc.getList("directed", Integer.class)).size();
								if (directorDoc != null)
									directedFromAgg = directorDoc.getInteger("total");
								
								if (directedFromDB != expectedDirector.get(director) || 
										directedFromAgg != expectedDirector.get(director)) {
									reasons.append("The total number of directed movies for director "+director+
											" was not as expected:"+directedFromDB+"; ");
									dataIssues.set(true);
								}
							}
							
							
							for (String genre : expectedGenre.keySet()) {
								q.add(Document.parse("{ $match : { genres : '"+genre+"' } }"));
								q.add(Document.parse("{ $count : 'cnt' }"));
								
								Document countDoc = movies.aggregate(q).first();
								q.clear();
								
								if (countDoc == null) {
									reasons.append("Count of genre " + genre + " did not exist; ");
									dataIssues.set(true);
									continue;
								}
								
								int genreCount = countDoc.getInteger("cnt");
								
								if (genreCount != expectedGenre.get(genre)) {
									reasons.append("Count of movies of genre " + genre + " was not as expected:"+genreCount+"; ");
									dataIssues.set(true);
								}
							}
						}
						
						if (schemaIssues.get()) {
							System.out.println("There were schema issues.");
							System.out.println("\tPenalty: -10");
						}
						
						if (dataIssues.get()) {
							System.out.println("There were data issues.");
							System.out.println("\tPenalty: -5");
						}
						
						if (reasons.length() > 0)
							System.out.println("Reasons: " + reasons);
						
						client.close();
					} catch (Exception oops) {
						System.out.println("A major problem happened!");
						oops.printStackTrace(System.out);
					} finally {
						destroyProcess(process);
					}
				} else
					System.out.println("Folder not found!");
			} catch (Throwable oops) {
				System.out.println("Something went really wrong!");
				oops.printStackTrace(System.out);
			}
			
			System.out.println();
			System.out.println();
		}

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
	
	public static boolean runProcess(String[] command, int waitInMin) {
		boolean ret = false;
		ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
		Process process = null;
		try {
			process = builder.start();
			StreamGobbler gobbler = new GradeIMDBSQLToMongo().new StreamGobbler(process.getInputStream(), true);
			gobbler.start();
			ret = !process.waitFor(5, TimeUnit.MINUTES);
		} catch (Exception oops) {
			System.out.println("A major problem happened: " + oops.getMessage() + "; ");
			ret = true;
		} finally {
			builder = null;
			if (process != null)
				process.destroy();
		}
		return ret;
	}
	
	public static void destroyProcess(Process process) {
		if (process != null) {
			process.descendants().forEach(d -> {d.destroy();});
			process.destroy();
		}
	}
	
	public class StreamGobbler extends Thread {
		private InputStream is;
		private boolean print;

		public StreamGobbler(InputStream is, boolean print) {
			this.is = is;
			this.print = print;
		}

		public void run() {
			try {
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				String line = null;
				while ((line = br.readLine()) != null)
					if (print)
						System.out.println(line);
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
	}

}
