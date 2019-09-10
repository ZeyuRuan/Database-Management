package simpledb.clients;

import java.sql.*;
import java.util.HashMap;
import java.util.List;

import simpledb.remote.SimpleConnection;
import simpledb.remote.SimpleDriver;
import simpledb.remote.SimpleStatement;
import simpledb.server.SimpleDB;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import au.com.bytecode.opencsv.CSVReader;

public class StudentTester {
    private static SimpleConnection conn = null;
    public static final String PUB_PATH = "/course/cs1270/pub/optimization/tests/";

    public static void main(String[] args) {
	   try {
			SimpleDriver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			
			HashMap<String, Integer> csvFields = new HashMap<String, Integer>();
			csvFields.put("id", 0);
			csvFields.put("name", 1);
			csvFields.put("stars", 2);
			
			String brcmd = "select id, name, stars from business join review on bid = id";
			String rucmd = "select id, name, stars from review join user on uid = id";

			System.out.println("***********************");
			System.out.println("NESTED LOOP TESTS");
			System.out.println("***********************");
			System.out.print("business/review join: ");
			doQuery(brcmd, "NestedLoopBR.csv", csvFields, 1);
			System.out.print("review/user join: ");
			doQuery(rucmd, "NestedLoopRU.csv", csvFields, 1);
			System.out.println();
			
			System.out.println("***********************");
			System.out.println("BLOCK NESTED LOOP TESTS");
			System.out.println("***********************");
			System.out.print("business/review join: ");
			doQuery(brcmd, "BlockNestedLoopBR.csv", csvFields, 2);
			System.out.print("review/user join: ");
			doQuery(rucmd, "BlockNestedLoopRU.csv", csvFields, 2);
	    } catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void doQuery(String cmd, String file, HashMap<String, Integer> csvFields, int joinType) {
		ResultSet rs = null;
		try {
		    SimpleStatement stmt = conn.createStatement();
		    rs = stmt.executeQuery(cmd, joinType);
		    ResultSetMetaData md = rs.getMetaData();
		    int numcols = md.getColumnCount();

		    CSVReader resultReader = new CSVReader(new FileReader(PUB_PATH + file));
		    List<String[]> resultEntries = resultReader.readAll();
		    resultReader.close();
		    
		    int idx = 0;

		    while (rs.next()) {
		    	if (idx == resultEntries.size()) {
		    		System.out.println("Number of records in result exceeds number "
		    				+ "of records in solution");
		    		return;
		    	}
		    	
				for (int i=1; i<=numcols; i++) {
					String fldname = md.getColumnName(i);
					int resultcol = csvFields.get(fldname);
					int fldtype = md.getColumnType(i);
					String fld = "";
					
					if (fldtype == Types.INTEGER) {
						fld = Integer.toString(rs.getInt(fldname));
					} else {
						fld = rs.getString(fldname);
					}
					
					if (!fld.equals(resultEntries.get(idx)[resultcol].replaceAll("\\ufeff", ""))) {
						System.out.println("Mismatch on row " + i + " of "
								+ "solution");
						return;							
					}
				}
				idx++;
			}
		    
		    if (idx != resultEntries.size()) {
		    	System.out.println("Number of records in result falls short of "
		    			+ "number of records in solution");
		    	return;
		    }
		    
		    System.out.println("Result matches solution");
		} catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
			} catch (SQLException e) {}
		}
	}
}