package edu.rit.ibd.a3;

import java.io.File;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

public class FDDiscovery {

	public static void main(String[] args) throws Exception {
		final String url = args[0];
		final String user = args[1];
		final String pwd = args[2];
		final String relationName = args[3];
		final String outputFile = args[4];

		//System.out.println(url + " " + user + " " + pwd + " " + relationName + " " + outputFile);
		
		Connection con = DriverManager.getConnection(url, user, pwd);

		//System.out.println(con);
		
		// These are the attributes of the input relation.
		Set<String> attributes = new HashSet<>();
		// These are the functional dependencies discovered.
		Set<FunctionalDependency> fds = new HashSet<>();
		
		// TODO 0: Your code here!
		
		// Your program must be generic and work for any relation provided as input. You must read the names of the attributes from the input relation and store
		//	them in the attributes set.
		//
		// You must traverse the lattice of attributes starting from combinations of size 1. Remember that all the functional dependencies we will discover are
		//	of the form a1, ..., ak -> aj; therefore, there is a single attribute on the right-hand side, and one or more attributes on the left-hand side. Re-
		//	member also that we are not interested in trivial functional dependencies (right-hand side is included in left-hand side) or non-minimal (there exi-
		//	sts another functional dependency that is contained in the current one).
		//
		// To traverse the lattice, we start from single combinations of attributes in the left-hand side, then combinations of two attributes, then combinatio-
		//	ns of three attributes, etc. We stop when we have tested all possible combinations. Use Sets.combinations to generate these combinations.
		//

		
		// Read attributes. Use the metadata to get the column info.
		PreparedStatement st = con.prepareStatement("SELECT * FROM " + relationName + " LIMIT 1");
		ResultSet rs = st.executeQuery();
		int columnCount = rs.getMetaData().getColumnCount();
		for (int i = 1; i <= columnCount; i++)
			attributes.add(rs.getMetaData().getColumnName(i));
		rs.close();
		st.close();

		//System.out.println(attributes.size());
		Set<String> attributesRight = new HashSet<>(attributes);
		
		// Each FD has a left-hand side and a right-hand side. For LHS, start from size one and keep increasing.
		for (int size = 1; size < columnCount; size++) {
			// Get each combination of attributes in the left-hand side of the appropriate size.
			for (Set<String> leftHandSide : Sets.combinations(attributes, size)) {
				// Get the attributes in the right-hand side.
				//System.out.println(leftHandSide);
				for (String rightAttribute : attributesRight) {

					FunctionalDependency candidateFD = new FunctionalDependency(leftHandSide, rightAttribute);
					//System.out.println(candidateFD);
					if (isTrivial(candidateFD)) {			// SKIP IF TRIVIAL
						continue;
					} else if (!isMinimal(candidateFD, fds)){
						//System.out.println("HERE");
						continue;
					} else if (isValidFunctionalDependency(candidateFD, con, relationName)) {
						//System.out.println("HERE2");
						fds.add(candidateFD);
					}

				}
				
				// Form the candidate FD using both left-hand and right-hand sides.
				
				// Make sure that the candidate FD is not trivial and minimal.
				
				// Make sure that the candidate FD is an FD. Build a SQL query to check it.
				
				// If it is an FD, add it to the set of discovered FDs.
			}
		}
		
		// TODO 0: End of your code.
			
		// Write to file!

		PrintWriter writer = new PrintWriter(new File(outputFile));
		for (Object fd : fds)
			writer.println(fd);
		writer.close();

		con.close();
	}

	private static boolean isValidFunctionalDependency(FunctionalDependency candidateFD, Connection con, String relationName) throws SQLException {
		// a1, a2 -> a3 is a functional dependency for relation x if the following SQL query outputs no result:
		//	SELECT * FROM x AS t1 JOIN x AS t2 ON t1.a1 = t2.a1 AND t1.a2 = t2.a2 WHERE t1.a3 <> t2.a3
		//
		//	You must compose this type of SQL for the different combinations of attributes to find functional dependencies.

		//System.out.println("-------------IN----------------");

		StringBuilder query1StPart = new StringBuilder("SELECT * FROM " + relationName + " AS t1 JOIN " + relationName + " AS t2 ON ");
		String query2NdPart = "";
		StringBuilder query3RdPart = new StringBuilder(" WHERE t1." + candidateFD.rightHandSide + " <> t2." + candidateFD.rightHandSide + " LIMIT 10");

		String queryConstant1 = "t1.";
		String queryConstant2 = " = t2.";

		ArrayList<String> joinAttributes = new ArrayList<>();

		int count = 0;
		for (String attr : candidateFD.leftHandSide) {
			joinAttributes.add(queryConstant1 + attr + queryConstant2 + attr + " AND ");
		}

		String temp = joinAttributes.get(joinAttributes.size()-1);
		temp = temp.substring(0, temp.length() - 4);
		joinAttributes.set(joinAttributes.size()-1, temp);

		for (String e : joinAttributes) {
			query2NdPart += e;
		}
		String finalQuery = query1StPart + query2NdPart + query3RdPart;
		//System.out.println(finalQuery);

		PreparedStatement st = con.prepareStatement(finalQuery);
		ResultSet rs = st.executeQuery();
		rs.last();
		int size = rs.getRow();
		//System.out.println(size);
		return size == 0;
	}

	private static boolean isMinimal(FunctionalDependency candidateFD, Set<FunctionalDependency> functionalDependencySet) {
		boolean result = true;
		//System.out.println("2");
		if (!functionalDependencySet.isEmpty()) {
			for (FunctionalDependency establishedFD : functionalDependencySet) {
				if (candidateFD.contains(establishedFD)) {
					result = false;
				}
			}
		}
		return result;
	}

	private static boolean isTrivial(FunctionalDependency candidateFD) {
		//System.out.println("1");
		return candidateFD.leftHandSide.contains(candidateFD.rightHandSide);
	}

	private static boolean isTrivial(Set<String> leftHandSide, String rightAttribute) {
		return leftHandSide.contains(rightAttribute);
	}

}

class FunctionalDependency {
	Set<String> leftHandSide;
	String rightHandSide;

	public FunctionalDependency(Set<String> leftHandSide, String rightHandSide) {
		this.leftHandSide = leftHandSide;
		this.rightHandSide = rightHandSide;
	}


	@Override
	public String toString() {
		Collections.sort(this.leftHandSide.stream().collect(Collectors.toList()), String::compareTo);

		return String.join(", ", this.leftHandSide)+ " -> " + this.rightHandSide;
	}

	public boolean contains(FunctionalDependency establishedFD) {
		boolean result = false;
		Set<String> establishedLHS = establishedFD.leftHandSide;
		String establishedRHS = establishedFD.rightHandSide;
		if (this.leftHandSide.containsAll(establishedLHS) && this.rightHandSide.equals(establishedRHS)) {
			result = true;
		}
		return result;
	}
}
