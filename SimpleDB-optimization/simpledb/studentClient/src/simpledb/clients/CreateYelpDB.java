package simpledb.clients;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

import au.com.bytecode.opencsv.CSVReader;
import simpledb.remote.SimpleDriver;

public class CreateYelpDB {
    public static void main(String[] args) {
		Connection conn = null;
		try {
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			Statement stmt = conn.createStatement();
			String path = "/course/cs1270/pub/optimization/";
			
			String s = "create table BUSINESS(Id varchar(25), Name varchar(25), "
					+ "City varchar(20), State varchar(2), Reviews int)";
			stmt.executeUpdate(s);
			System.out.println("business table created.");
		
			String delete = "delete from BUSINESS";
			stmt.executeUpdate(delete);
			
			String insertb = "insert into BUSINESS(Id, Name, City, State, Reviews) "
					+ " values ";
			boolean[] b_str = new boolean[]{true, true, true, true, false};
			CSVReader breader = new CSVReader(new FileReader(path + "business.csv"));
			String[] record;
			
			while ((record = breader.readNext()) != null) {
				StringBuilder sb = new StringBuilder();
				sb.append("(");
				
				for (int i = 0; i < record.length; i++) {
					if (b_str[i]) sb.append("'");
					sb.append(record[i].trim().replace("'", ""));
					if (b_str[i]) sb.append("'");
					if (i < record.length - 1) {
						sb.append(",");
					}
				}
				sb.append(")");
				stmt.executeUpdate(insertb + sb.toString());
			}
			
			breader.close();
					    
			System.out.println("business records inserted.");
			
			// ----------------------------------------------
			
			s = "create table REVIEW(Bid varchar(25), Uid varchar(25), "
				+ "Stars int)";
			stmt.executeUpdate(s);
			System.out.println("review table created.");
			
			delete = "delete from REVIEW";
			stmt.executeUpdate(delete);
			
			String insertr = "insert into REVIEW(Bid, Uid, Stars) values ";
			
			boolean[] r_str = new boolean[]{true, true, false};		
			CSVReader rreader = new CSVReader(new FileReader(path + "review.csv"));
			
			while ((record = rreader.readNext()) != null) {
				StringBuilder sb = new StringBuilder();
				sb.append("(");
				
				for (int i = 0; i < record.length; i++) {
					if (r_str[i]) sb.append("'");
					sb.append(record[i].trim().replace("'", ""));
					if (r_str[i]) sb.append("'");
					if (i < record.length - 1) {
						sb.append(",");
					}
				}
				sb.append(")");
				stmt.executeUpdate(insertr + sb.toString());
			}
			
			rreader.close();
			
			System.out.println("review records inserted.");
			
			// ----------------------------------------------
			
			s = "create table USER(id varchar(25), name varchar(25), "
				+ "reviews int, useful int, funny int, cool int)";
			stmt.executeUpdate(s);
			System.out.println("user table created.");
			
			delete = "delete from USER";
			stmt.executeUpdate(delete);
			
			String insertu = "insert into USER(id, name, reviews, useful, "
					+ "funny, cool) values ";
			
			boolean[] u_str = new boolean[]{true, true, false, false, false, false};		
			CSVReader ureader = new CSVReader(new FileReader(path + "user.csv"));
			
			while ((record = ureader.readNext()) != null) {
				StringBuilder sb = new StringBuilder();
				sb.append("(");
				
				for (int i = 0; i < record.length; i++) {
					if (u_str[i]) sb.append("'");
					sb.append(record[i].trim().replace("'", ""));
					if (u_str[i]) sb.append("'");
					if (i < record.length - 1) {
						sb.append(",");
					}
				}
				sb.append(")");
				stmt.executeUpdate(insertu + sb.toString());
			}
			
			ureader.close();
			
			System.out.println("user records inserted.");		

		}
		catch(SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		finally {
			try {
				if (conn != null)
					conn.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
