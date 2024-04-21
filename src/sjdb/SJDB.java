/**
 * 
 */
package sjdb;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author nmg
 *
 */
public class SJDB {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// read serialised catalogue from file and parse
		String catFile = args[0];
		Catalogue cat = new Catalogue();
		Inspector inspector = new Inspector();
		CatalogueParser catParser = new CatalogueParser(catFile, cat);
		catParser.parse();
		
		// read stdin, parse, and build canonical query plan
		QueryParser queryParser = new QueryParser(cat, new InputStreamReader(System.in));
		//Operator plan = queryParser.parse();

		Operator plan = query(cat);

		// create estimator visitor and apply it to canonical plan
		Estimator est = new Estimator();
		plan.accept(est);
		plan.accept(inspector);

		System.out.println("\n");

		// create optimised plan
		Optimiser opt = new Optimiser(cat);
		Operator optPlan = opt.optimise(plan);
		optPlan.accept(est);
		optPlan.accept(inspector);
	}

	public static Catalogue createCatalogue() {
		Catalogue cat = new Catalogue();
		cat.createRelation("Person", 400);
		cat.createAttribute("Person", "persid", 400);
		cat.createAttribute("Person", "persname", 350);
		cat.createAttribute("Person", "age", 47);
		cat.createRelation("Project", 40);
		cat.createAttribute("Project", "projid", 40);
		cat.createAttribute("Project", "projname", 35);
		cat.createAttribute("Project", "deptname", 5);
		cat.createRelation("Department", 5);
		cat.createAttribute("Department", "deptid", 5);
		cat.createAttribute("Department", "deptname", 5);
		cat.createAttribute("Department", "manager", 5);

		return cat;
	}

	public static Operator query(Catalogue cat) throws Exception {
		Scan Person = new Scan(cat.getRelation("Person"));
		Scan Project = new Scan(cat.getRelation("Department"));
		Scan Department = new Scan(cat.getRelation("Project"));

		Product p1 = new Product(Project, Person);
		Product p2 = new Product(Department, p1);


		Select s1 = new Select(p2, new Predicate(new Attribute("persid"), new Attribute("manager")));
		Select s2 = new Select(s1, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		Select s3 = new Select(s2, new Predicate(new Attribute("persname"), "Smith"));

		Project project = new Project(s1, Arrays.asList(new Attribute("projname"), new Attribute("deptname")));
		return s3;
	}
}
