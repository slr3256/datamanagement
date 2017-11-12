
/*
export CLASSPATH=postgresql.jar:.
javac test.java
java test
*/

import java.sql.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class test {

	public static void createTable(Connection conn) throws SQLException {
		System.out.println("creating table");
	    String createString = 
	    	"create table mytable ("+ 
	     	"theKey int PRIMARY KEY," +
	     	"columnA int," + 
	     	"columnB int, " +
	     	"filler CHAR(247))" ;
	    Statement stmt = conn.createStatement();
	    stmt.executeUpdate(createString);
	    stmt.close();
	}


	public static void OneTable(Connection conn) throws SQLException {
		// use batch insertion without autocommit to insert more rows at a time
		// System.out.println("inserting row");
		int n = 0;
		String alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ abcdefghijklmnopqrstuvwxyz";
		int Slength = alpha.length();

		while(n<10000) {
			Statement stmt = conn.createStatement();
			for (int i = 0; i < 1000; i++){
				int randomNum1 = ThreadLocalRandom.current().nextInt(0, 50001);
				int randomNum2 = ThreadLocalRandom.current().nextInt(0, 50001);
				String random3 = "";
				for (int x = 0; x < 247; x++){
					int randomIndex = ThreadLocalRandom.current().nextInt(0, Slength-1);
					random3 += alpha.charAt(randomIndex);
				}
			    String insertString = 
			    	"insert into mytable (theKey, columnA, columnB, filler)"+ 
			     	"values (" + n + ", " + randomNum1 + ", " + randomNum2 + ", '" + random3 + "')";
				stmt.addBatch(insertString);
				n++;
			}
			stmt.executeBatch();
			stmt.close();
		}
	    
	}

	//creates table with Variation II
	public static void TwoTable(Connection conn) throws SQLException {
		// use batch insertion without autocommit to insert more rows at a time
		// System.out.println("inserting row");
		int n = 0;
		String alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ abcdefghijklmnopqrstuvwxyz";
		int Slength = alpha.length();

		List<Integer> keyList = new ArrayList<>(10000);
	  	for (int i=0; i<=10000; i++) {
	    	keyList.add(i);
  		}
  		Collections.shuffle(keyList);
		while(n<10000) {
			Statement stmt = conn.createStatement();
			for (int i = 0; i < 1000; i++){
				int randomNum1 = ThreadLocalRandom.current().nextInt(0, 50001);
				int randomNum2 = ThreadLocalRandom.current().nextInt(0, 50001);
				String random3 = "";
				for (int x = 0; x < 247; x++){
					int randomIndex = ThreadLocalRandom.current().nextInt(0, Slength-1);
					random3 += alpha.charAt(randomIndex);
				}
			    String insertString = 
			    	"insert into mytable (theKey, columnA, columnB, filler)"+ 
			     	"values (" + keyList.get(n) + ", " + randomNum1 + ", " + randomNum2 + ", '" + random3 + "')";
				stmt.addBatch(insertString);
				n++;
			}
			stmt.executeBatch();
			stmt.close();
		}
	    
	}

	//creates table with secondary index on columnA
	public static void indexA(Connection conn) throws SQLException {
		System.out.println("creating secondary index");
	    String indexAString = 
	    	"create index colA on mytable (columnA)";
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(indexAString);
	    stmt.close();
	}

	//creates table with secondary index on columnA
	public static void indexB(Connection conn) throws SQLException {
		System.out.println("creating secondary index");
	    String indexBString = 
	    	"create index colB on mytable (columnB)";
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(indexBString);
	    stmt.close();
	}

	//Execute query I 
	public static void queryOne(Connection conn, int num) throws SQLException {
		System.out.println("printing table");
	    String selectString = 
	    	"select * from mytable where mytable.columnA = " + num;
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(selectString);
		rs.close();
	    stmt.close();
	}

	//Execute query II 
	public static void queryTwo(Connection conn, int num) throws SQLException {
		System.out.println("printing table");
	    String selectString = 
	    	"select * from mytable where mytable.columnB = "+ num;
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(selectString);
		rs.close();
	    stmt.close();
	}

	//Execute query II 
	public static void queryThree(Connection conn, int num) throws SQLException {
		System.out.println("printing table");
	    String selectString = 
	    	"select * from mytable where mytable.columnA = " + num + " and mytable.columnB = " + num;
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(selectString);
		rs.close();
	    stmt.close();
	}

	public static void printTable(Connection conn) throws SQLException {
		System.out.println("printing table");
	    String selectString = 
	    	"select * from mytable";
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(selectString);
		while (rs.next()) {
	    	System.out.println(rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + "," + rs.getString(4));
		}
		rs.close();
	    stmt.close();
	}


	public static void dropTable(Connection conn) throws SQLException {
		System.out.println("dropping table");
	    String dropString = 
	    	"drop table mytable";
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(dropString);
	    stmt.close();
	}


	public static void main(String[] args) throws SQLException, ClassNotFoundException {

		System.out.println("loading driver");
		Class.forName("org.postgresql.Driver");
		System.out.println("driver loaded");

		System.out.println("Connecting to DB");
		Connection conn = DriverManager.getConnection("jdbc:postgresql:postgres", "postgres", "bl00berrie");
		System.out.println("Connected to DB");

		try {
			// drops if there
			dropTable(conn);
		}
		catch (SQLException e) {}

		//loading Variation I
		long start = System.currentTimeMillis();
		createTable(conn);
		OneTable(conn);
		//printTable(conn);
		long end = System.currentTimeMillis();
		long total = end - start;
		System.out.println("VARATION I LOAD TIME : " + total);

		//query list 
		//int[] intArray = new int[]{ 25000, 10000, 120820, 100000, 16, 1000000, 2300000, 130, 1800345, 160000 };
		int[] intArray = new int[]{ 10, 1000, 1200, 500, 2500, 100, 2300, 8000, 7050, 1600 };
		
		//query I with physical organization 1
		long avg;
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryOne(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY I FOR PHYSICAL ORGANIZATION 1: " + avg);

		//Query II for physical organization 1
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryTwo(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY II FOR PHYSICAL ORGANIZATION 1: " + avg);

		//Query III for physical organization 1
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryThree(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY III FOR PHYSICAL ORGANIZATION 1: " + avg);		
		//add index for col a
		start = System.currentTimeMillis();
		indexA(conn);
		end = System.currentTimeMillis();
		total = end - start;
		System.out.println ("TIME TO ADD INDEX A: " + total);
		//Query I with physical organization 2 
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryOne(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY I FOR PHYSICAL ORGANIZATION 2: " + avg);
		//Query II with physical organization 2
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryTwo(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY II FOR PHYSICAL ORGANIZATION 2: " + avg);
		//Query III with pysical organization 2
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryThree(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY III FOR PHYSICAL ORGANIZATION 1: " + avg);

		dropTable(conn);
		createTable(conn);
		OneTable(conn);

		//add secondaty index for col b
		start = System.currentTimeMillis();
		indexB(conn);
		end = System.currentTimeMillis();
		total = end - start;
		System.out.println ("TIME TO ADD INDEX B: " + total);
		//Query I with physical organization 3 
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryOne(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY I FOR PHYSICAL ORGANIZATION 3: " + avg);
		//Query II with physical organization 3
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryTwo(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY II FOR PHYSICAL ORGANIZATION 3: " + avg);
		//Query III with pysical organization 3
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryThree(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY III FOR PHYSICAL ORGANIZATION 3: " + avg);
		//add secondary index for col a
		indexA(conn);
		///Query I with physical organization 4 
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryOne(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY I FOR PHYSICAL ORGANIZATION 4: " + avg);
		//Query II with physical organization 3
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryTwo(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY II FOR PHYSICAL ORGANIZATION 4: " + avg);
		//Query III with pysical organization 3
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryThree(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY III FOR PHYSICAL ORGANIZATION 4: " + avg);

		dropTable(conn);
		//loading Variation II
		start = System.currentTimeMillis();
		createTable(conn);
		TwoTable(conn);
		//printTable(conn);
		dropTable(conn);
		end = System.currentTimeMillis();
		total = end - start;
		System.out.println("VARIATION II LOAD TIME : " + total);


		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryOne(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY I FOR PHYSICAL ORGANIZATION 1: " + avg);

		//Query II for physical organization 1
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryTwo(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY II FOR PHYSICAL ORGANIZATION 1: " + avg);

		//Query III for physical organization 1
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryThree(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY III FOR PHYSICAL ORGANIZATION 1: " + avg);		
		//add index for col a
		start = System.currentTimeMillis();
		indexA(conn);
		end = System.currentTimeMillis();
		total = end - start;
		System.out.println ("TIME TO ADD INDEX A: " + total);
		//Query I with physical organization 2 
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryOne(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY I FOR PHYSICAL ORGANIZATION 2: " + avg);
		//Query II with physical organization 2
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryTwo(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY II FOR PHYSICAL ORGANIZATION 2: " + avg);
		//Query III with pysical organization 2
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryThree(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY III FOR PHYSICAL ORGANIZATION 1: " + avg);

		dropTable(conn);
		createTable(conn);
		OneTable(conn);

		//add secondaty index for col b
		start = System.currentTimeMillis();
		indexB(conn);
		end = System.currentTimeMillis();
		total = end - start;
		System.out.println ("TIME TO ADD INDEX B: " + total);
		//Query I with physical organization 3 
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryOne(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY I FOR PHYSICAL ORGANIZATION 3: " + avg);
		//Query II with physical organization 3
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryTwo(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY II FOR PHYSICAL ORGANIZATION 3: " + avg);
		//Query III with pysical organization 3
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryThree(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY III FOR PHYSICAL ORGANIZATION 3: " + avg);
		//add secondary index for col a
		indexA(conn);
		///Query I with physical organization 4 
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryOne(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY I FOR PHYSICAL ORGANIZATION 4: " + avg);
		//Query II with physical organization 3
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryTwo(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY II FOR PHYSICAL ORGANIZATION 4: " + avg);
		//Query III with pysical organization 3
		for (int i = 1; i <= 10; i ++) {
			start = System.currentTimeMillis();
			queryThree(conn, intArray[i-1]);
			end = System.currentTimeMillis();
			total = end - start;
			avg = new long[((int) avg * i +(int) total)/(i+1)];
		}
		System.out.println("QUERY III FOR PHYSICAL ORGANIZATION 4: " + avg);

		dropTable(conn);

	}

}