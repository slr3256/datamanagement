
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


	public static void insertRow(Connection conn) throws SQLException {
		// use batch insertion without autocommit to insert more rows at a time
		System.out.println("inserting row");
		int n = 0;
		String alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ abcdefghijklmnopqrstuvwxyz";
		int Slength = alpha.length();

		List<Integer> keyList = new ArrayList<>(3000000);
	  	 	for (int i=0; i<=3000000; i++) {
	    	keyList.add(i);
  		}
  		Collections.shuffle(keyList);
		while(n<3000000) {
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

		long start = System.currentTimeMillis();
		createTable(conn);
		insertRow(conn);
		printTable(conn);
		dropTable(conn);
		long end = System.currentTimeMillis();
		long total = end - start;
		System.out.println("TOTAL TIME : " + total);

	}

}