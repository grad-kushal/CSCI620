package edu.rit.ibd.a3;

import com.google.common.collect.Sets;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

public class CKDiscovery {
	
	public static void main(String[] args) throws Exception {
		final String relation = args[0];
		final String fdsStr = args[1];
		final String outputFile = args[2];

		Relation r = new Relation(relation.replaceAll("\\s", ""), fdsStr.replaceAll("\\s", ""));
		
		// This stores the attributes of the input relation.
		Set<String> attributes = new HashSet<>();
		attributes = r.getAttributes();
		// This stores the functional dependencies provided as input.
		Set<FunctionalDependency> fds = new HashSet<>();
		fds = r.getFunctionalDependencies();

		////System.out.println(attributes);
		//System.out.println(r);
		// This stores the candidate keys discovered; each key is a set of attributes.
		List<Set<String>> keys = new ArrayList<>();
		
		// TODO 0: Your code here!

		ArrayList<HashSet<String>> customCase = new ArrayList<>(4);
		for (int i = 0; i < 4; i++) {
			customCase.add(new HashSet<>());
		}

		for (String a : attributes) {
			int flag = 1, lflag = 0, rflag = 0;
			for (FunctionalDependency ff : fds) {
				if (ff.leftHandSide.contains(a)) {
					lflag = 1;
					break;
				}
			}
			for (FunctionalDependency ff : fds) {
				if (ff.rightHandSide.contains(a)) {
					rflag = 1;
					break;
				}
			}
			if (lflag == 1 && rflag == 1) {
				flag = 4;
			} else if (lflag == 1 && rflag == 0) {
				flag = 3;
			} else if (lflag == 0 && rflag == 1) {
				flag = 2;
			} else {
				flag = 1;
			}
			customCase.get(flag-1).add(a);
		}
		////System.out.println(customCase);

		Set<String> core = Sets.union(customCase.get(0), customCase.get(2));

		////System.out.println("Core: " + core);
		// Compute the closure of the core.
		Set<String> closure = computeClosure(fds, core);
		////System.out.println("Closure: " + closure);
		
		if (closure.size() == attributes.size()) {
			// Add key.
			keys.add(core);
		}
		else {
			// If not, use Sets.combinations to find all possible combinations of attributes.
			for (int size = 1; size <= customCase.get(3).size(); size++) {
				boolean skFlag = true;
				for (Set<String> comb : Sets.combinations(customCase.get(3), size)) {
					Set<String> X = Sets.union(comb, core);
					boolean ckFlag = false;
					int discoveredFlag = 0;
					Set<String> xClosure = computeClosure(fds, X);
					////System.out.println( X + " : " + xClosure);
					if (xClosure.size() == attributes.size()) {
						skFlag = true;
						for (Set<String> k : keys) {
							////System.out.println(k + " : " + X);
							if (X.containsAll(k)) {
								discoveredFlag = 1;
							}
						}
						if (discoveredFlag == 0) {
							ckFlag = true;
							keys.add(X);
						}
					}
				}
			}
		}
		// If the closure of the core does not contain all the attributes, proceed to combine attributes.
		//
		// For each combination of attributes starting from size 1 classified as case 4:
		//	X = comb union core
		//	If the closure of X contains all attributes of the input relation:
		//		X is superkey
		//		If X is not contained in a previous candidate key already discovered:
		//			X is a candidate key
		//	If all the combinations of size k are superkeys -> Stop


		////System.out.println(keys);
		// TODO 0: End of your code.
		
		PrintWriter writer = new PrintWriter(new File(outputFile));
		for (Set<String> key : keys)
			writer.println(key.stream().sorted().collect(java.util.stream.Collectors.toList()).
					toString().replace("[", "").replace("]", ""));
		writer.close();
	}
	
	private static Set<String> computeClosure(Set<FunctionalDependency> fds, Set<String> core) {
		Set<String> result = new HashSet<>(core);
		int flag = 0;
		do {
			flag = 0;
			for (FunctionalDependency functionalDependency : fds) {
				if (result.containsAll(functionalDependency.leftHandSide) &&
						! result.containsAll(functionalDependency.rightHandSide)) {
					result = Sets.union(result, functionalDependency.rightHandSide);
					flag = 1;
				}
				////System.out.println(result);
			}
		} while (flag == 1);
		return result;
	}

}

class FunctionalDependency {
	TreeSet<String> leftHandSide;
	TreeSet<String> rightHandSide;

	public FunctionalDependency(TreeSet<String> leftHandSide, TreeSet<String> rightHandSide) {
		this.leftHandSide = leftHandSide;
		this.rightHandSide = rightHandSide;
	}

	public FunctionalDependency(String fdStr) {
		//A,B,C->D;A,B,C->D,E;D->A,B;E->A,C
		String [] fd = fdStr.split("->");
		String [] lhs = fd[0].split(",");
		String [] rhs = fd[1].split(",");
		this.leftHandSide = new TreeSet<>(List.of(lhs));
		this.rightHandSide = new TreeSet<>(List.of(rhs));

	}

	public boolean isTrivial() {
		return this.leftHandSide.containsAll(this.rightHandSide);
	}

	public boolean isMinimal(Set<FunctionalDependency> functionalDependencySet) {
		boolean result = true;
		////System.out.println("2");
		if (!functionalDependencySet.isEmpty()) {
			for (FunctionalDependency establishedFD : functionalDependencySet) {
				if (this.contains(establishedFD)) {
					result = false;
				}
			}
		}
		return result;
	}

	@Override
	public String toString() {
		Collections.sort(this.leftHandSide.stream().collect(Collectors.toList()), String::compareTo);
		Collections.sort(this.rightHandSide.stream().collect(Collectors.toList()), String::compareTo);

		return String.join(", ", this.leftHandSide) + " -> " + String.join(", ", this.rightHandSide);
	}

	public boolean contains(FunctionalDependency establishedFD) {
		boolean result = false;
		TreeSet<String> establishedLHS = establishedFD.leftHandSide;
		TreeSet<String> establishedRHS = establishedFD.rightHandSide;
		if (this.leftHandSide.containsAll(establishedLHS) && this.rightHandSide.containsAll(establishedRHS)) {
			result = true;
		}
		return result;
	}
}

class Relation {
	public TreeSet<String> getAttributes() {
		return attributes;
	}

	TreeSet<String> attributes;

	public HashSet<FunctionalDependency> getFunctionalDependencies() {
		return functionalDependencies;
	}

	HashSet<FunctionalDependency> functionalDependencies;

	public Relation(String attributes, String fdStr) {

		attributes.replaceAll("\\s","");
		fdStr.replaceAll("\\s","");

		////System.out.println(attributes);
		////System.out.println(fdStr);

		attributes = attributes.substring(attributes.indexOf("(") + 1);
		attributes = attributes.substring(0, attributes.indexOf(")"));

		String [] attributesArray = attributes.split(",");

		this.attributes = new TreeSet<>(Arrays.asList(attributesArray));

		this.functionalDependencies = new HashSet<>();
		for (String fd : fdStr.split(";")) {
			this.functionalDependencies.add(new FunctionalDependency(fd));
		}
	}


	@Override
	public String toString() {
		String result = "Relation: " + attributes + ", \nFDs: ";
		for (FunctionalDependency f : this.functionalDependencies) {
			result += f + "; ";
		}
		return result;
	}
}

