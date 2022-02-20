package edu.rit.ibd.a3;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

public class CKDiscovery {
	
	public static void main(String[] args) throws Exception {
		final String relation = args[0];
		final String fdsStr = args[1];
		final String outputFile = args[2];
		
		// This stores the attributes of the input relation.
		Set<String> attributes = new HashSet<>();
		// This stores the functional dependencies provided as input.
		Set<FunctionalDependency> fds = new HashSet<>();
		// This stores the candidate keys discovered; each key is a set of attributes.
		List<Set<String>> keys = new ArrayList<>();
		
		// TODO 0: Your code here!
		
		// Parse the input relation that include its attributes. Recall that relation and attribute names can be formed by multiple letters.
		//
		// Parse the input functional dependencies. Recall that attributes can be formed by multiple letters.
		//
		// For each attribute a, you must classify as case 1 (a is not in the functional dependencies), case 2 (a is only in the right-hand side),
		//	case 3 (a is only in the left-hand side), case 4 (a is in both left- and right-hand sides).
		//
		// Compute the core (cases 1 and 3) and check whether the core is candidate key based on closure.
		//
		// If the closure of the core does not contain all the attributes, proceed to combine attributes.
		//
		// For each combination of attributes starting from size 1 classified as case 4:
		//	X = comb union core
		//	If the closure of X contains all attributes of the input relation:
		//		X is superkey
		//		If X is not contained in a previous candidate key already discovered:
		//			X is a candidate key
		//	If all the combinations of size k are superkeys -> Stop
		
		
		// Parse attributes.
		attributes.add(null);
		
		// Parse FDs.
		fds.add(null);
		
		// Discover candidate keys.
		
		// Split attributes by case.
		Set<String> case1 = new HashSet<>(), case2 = new HashSet<>(), case3 = new HashSet<>(), case4 = new HashSet<>();
		
		// Find the core.
		Set<String> core = new HashSet<>();
		
		// Compute the closure of the core.
		Set<String> closure = computeClosure(fds, core);
		
		if (/* If all the attributes are present in the closure. */ closure)
			// Add key.
			keys.add(null);
		else {
			// If not, use Sets.combinations to find all possible combinations of attributes.
		}
		
		
		// TODO 0: End of your code.
		
		PrintWriter writer = new PrintWriter(new File(outputFile));
		for (Set<String> key : keys)
			writer.println(key.stream().sorted().collect(java.util.stream.Collectors.toList()).
					toString().replace("[", "").replace("]", ""));
		writer.close();
	}
	
	private static Set<String> computeClosure(Set<Object> fds, Set<String> set) {
		return new HashSet<>();
	}

}

class FunctionalDependency {
	HashSet<String> leftHandSide;
	HashSet<String> rightHandSide;

	public FunctionalDependency(HashSet<String> leftHandSide, HashSet<String> rightHandSide) {
		this.leftHandSide = leftHandSide;
		this.rightHandSide = rightHandSide;
	}


	@Override
	public String toString() {
		Collections.sort(this.leftHandSide.stream().collect(Collectors.toList()), String::compareTo);
		Collections.sort(this.rightHandSide.stream().collect(Collectors.toList()), String::compareTo);

		return String.join(", ", this.leftHandSide) + " -> " + String.join(", ", this.rightHandSide);
	}

	public boolean contains(FunctionalDependency establishedFD) {
		boolean result = false;
		HashSet<String> establishedLHS = establishedFD.leftHandSide;
		HashSet<String> establishedRHS = establishedFD.rightHandSide;
		if (this.leftHandSide.containsAll(establishedLHS) && this.rightHandSide.containsAll(establishedRHS)) {
			result = true;
		}
		return result;
	}
}

