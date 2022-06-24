package edu.rit.ibd.a4;

import java.util.*;
import java.util.Date;

import com.github.javafaker.Faker;
import com.mongodb.client.MongoCollection;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;


public class EmployeeInformationApplication {

	static String [] deps = {"Transportation", "Police", "Fire", "Family and Support", "Law", "Sanitation", "Health", "Aviation", "Finance", "Business Affairs", "Water Management", "Housing", "OEMC", "DAIS", "DMV"};
	public static ArrayList<String> departments = new ArrayList<>(Arrays.asList(deps));
	public static void main(String[] args) throws Exception {

		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);

		generateData(client, db);

		client.close();
	}

	private static void generateData(MongoClient client, MongoDatabase db) {
		Faker faker = new Faker();
		ArrayList<Employee> employees = new ArrayList<>();

		for (int i = 0; i < 1500; i++) {
			employees.add(new Employee(faker));
		}

		TreeMap<Integer, ArrayList<Employee>> subordinateLists = new TreeMap<>();

		for (int i = 1; i < 15; i++) {
			Employee employeeTemp = employees.get(i);
			employeeTemp.role = employeeTemp.role.replaceAll("Role", "President");
			employeeTemp.manager = 0;
			if (subordinateLists.get(employeeTemp.manager) == null)
				subordinateLists.put(employeeTemp.manager, new ArrayList<>());
			ArrayList<Employee> subordinates = subordinateLists.get(employeeTemp.manager);
			subordinates.add(employeeTemp);
			subordinateLists.put(employeeTemp.manager, subordinates);
		}

		for (int i = 15; i < 150; i++) {
			Employee employeeTemp = employees.get(i);
			employeeTemp.role = employeeTemp.role.replaceAll("Role", "Vice-President");
			employeeTemp.manager = 1 + i % 14;
			if (subordinateLists.get(employeeTemp.manager) == null)
				subordinateLists.put(employeeTemp.manager, new ArrayList<>());
			ArrayList<Employee> subordinates = subordinateLists.get(employeeTemp.manager);
			subordinates.add(employeeTemp);
			subordinateLists.put(employeeTemp.manager, subordinates);
			employeeTemp.department = employees.get(employeeTemp.manager).department;
		}

		for (int i = 150; i <1500; i++) {
			Employee employeeTemp = employees.get(i);
			employeeTemp.manager = 15 + i % 135;
			if (subordinateLists.get(employeeTemp.manager) == null)
				subordinateLists.put((employeeTemp.manager), new ArrayList<>());
			ArrayList<Employee> subordinates = subordinateLists.get(employeeTemp.manager);
			subordinates.add(employeeTemp);
			subordinateLists.put(employeeTemp.manager, subordinates);
			employeeTemp.department = employees.get(employeeTemp.manager).department;
		}

		for (Employee e : employees) {
			MongoCollection<Document> employeeCollection = db.getCollection("employee");
			Document d = new Document();
			d.append("employee_id", e.id);
			d.append("first_name", e.fname);
			d.append("last_name", e.lname);
			d.append("photo", e.photo);
			d.append("dob", e.dob);
			d.append("cell", e.cell);
			d.append("email", e.email);
			d.append("address", e.address);
			d.append("role", e.role);
			d.append("department", e.department);
			d.append("manager", e.manager);
			d.append("manages", e.manages);
			d.append("emergency_contact", e.emergency_contact);
			d.append("date_of_joining", e.joining);
			d.append("date_of_termination", e.termination);
			employeeCollection.insertOne(d);
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

}

class Employee {

	static int empId = 0;

	int id;
	String fname;
	String lname;
	String photo;
	Date dob;
	String cell;
	String email;
	String address;
	String role;
	String department;
	int manager;
	ArrayList<Long> manages;
	String emergency_contact;
	Date joining;
	Date termination;

	@Override
	public String toString() {
		return "Employee{" +
				"id=" + id +
				", fname='" + fname + '\'' +
				", lname='" + lname + '\'' +
				", photo='" + photo + '\'' +
				", dob=" + dob +
				", cell='" + cell + '\'' +
				", email='" + email + '\'' +
				", address='" + address + '\'' +
				", role='" + role + '\'' +
				", department='" + department + '\'' +
				", manager=" + manager +
				", manages=" + manages +
				", emergency_contact='" + emergency_contact + '\'' +
				", joining=" + joining +
				", termination=" + termination +
				'}';
	}

	public Employee(Faker faker) {
		this.id = empId++;
		this.fname = faker.name().firstName();
		this.lname = faker.name().lastName();
		this.address = faker.address().fullAddress();
		this.cell = faker.phoneNumber().cellPhone();
		this.department = (this.id == 0) ? "Executive" : EmployeeInformationApplication.departments.get((this.id% EmployeeInformationApplication.departments.size()));
		this.dob = faker.date().birthday();
		this.email = fname.toLowerCase() + "." + lname.toLowerCase() + "@dbsimail.com";
		this.emergency_contact = faker.phoneNumber().cellPhone();
		this.joining = faker.date().between(dob, new Date());
		this.manager = (this.id == 0) ? 9999 : -1;
		this.manages = new ArrayList<>();
		this.photo = null;
		this.role = (this.id == 0) ? "CEO" : getRole(department);
		this.termination = faker.date().between(joining, new Date());
	}

	private String getRole(String department) {
		return department + " Role";
	}
}