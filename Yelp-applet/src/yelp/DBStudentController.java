package yelp;
import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This is the class file you will have to modify. You should only have to modify this file
 * and nothing else.
 * 
 * You will have to connect to the yelp.db database given. Once you are connected to the database,
 * you can execute queries on that database. The result will be return in a ResultSet. Fill in the 
 * appropriate method for each query. 
 * 
 * @author 
 *
 */

/**
 * Below are some snippets of JDBC code that may prove useful
 * 
 * For more sample JDBC code, check out 
 * http://web.archive.org/web/20100814175321/http://www.zentus.com/sqlitejdbc/
 * 
 * ---
 * 
 *      // INITIALIZE THE CONNECTION
 *      Class.forName("org.sqlite.JDBC");
 *      Connection conn = DriverManager.getConnection("jdbc:sqlite:PATH_TO_DB_FILE");
 * ---
 * 
 * Using PreparedStatement:
 * 
 * public void someQuery(String businessID){
 * 		String query = "SELECT * from business WHERE id = ? ;";
 * 		PreparedStatement prep = conn.prepareStatement(query);
 * 		prep.setString(1, businessID);
 * 		ResultSet rs = prep.executeQuery();
 * 		while (rs.next()) {
 * 			System.out.println("id = " + rs.getString("id"));
 * 			System.out.println("name = " + rs.getString("name"));
 * 		}
 * 		rs.close();
 * }
 * 
 */

public class DBStudentController implements DBController {
	
	public DBStudentController() throws SQLException, ClassNotFoundException {
		// Initialize the connection.
		
		Class.forName("org.sqlite.JDBC");
		conn = 
				DriverManager.getConnection("jdbc:sqlite:yelp.db");
				
	}
	
	private Connection conn;

	/**
	 * This function is called for query 1
	 * 
	 * Get the businesses in Providence, RI that are still open. 
	 * Results should be sorted by review counts in descending order. Return top 7 businesses.
	 * 
	 * @input N/A
	 * @output Six columns - the business id, name, full address, review count, photo url, and stars of the business.
	 * 
	 * @return A List of BusinessObject containing the result to the query.  
	 * @throws SQLException
	 */
	
	@Override
	public List<BusinessObject> query1() throws SQLException {
		// Your code goes here. Refer to BusinessObject.java
		// FOR FULL CREDIT make sure to set the id, name, address, reviewCount, photoUrl and stars properties of your BusinessObjects
		
		String query = "SELECT * FROM business WHERE city = 'Providence' AND state = 'RI' AND open = 1 "
				+ "ORDER BY review_count DESC LIMIT 7;";
		
		PreparedStatement prep = conn.prepareStatement(query);
		ResultSet rs = prep.executeQuery();
		
		List<BusinessObject> result1 = new ArrayList<BusinessObject>();
		
		while (rs.next()) {
			BusinessObject obj = new BusinessObject();
			obj.setId(rs.getString("id"));
			obj.setName(rs.getString("name"));
			obj.setAddress(rs.getString("full_address"));
			obj.setReviewCount(rs.getInt("review_count"));
			obj.setPhotoUrl(rs.getString("photo_url"));
			obj.setStars(rs.getDouble("stars"));
			result1.add(obj);
		}
		rs.close();
		
		return result1;		
		
	}

	/**
	 * This function is called for query 2
	 * 
	 * Get the reviews for a particular business, given the business ID. 
	 * Results should be sorted by the review's useful vote counts in descending order. Return top 7 reviews.
	 * 
	 * @input businessID
	 * @output Four columns - the user id, name of the user, stars of the review, and text of the review.
	 * 
	 * 
	 * @return A List of ReviewObject containing the result to the query
	 * @throws SQLException
	 */
	@Override
	public List<ReviewObject> query2(String businessID) throws SQLException {
		// Your code goes here. Refer to ReviewObject.java
		// FOR FULL CREDIT make sure to set the id, name, stars, text properties of your ReviewObjects

		String query = "SELECT r.user_id, u.name, r.stars, r.text FROM review AS r INNER JOIN user AS u "
				+ "ON r.user_id = u.id WHERE r.business_id = ? ORDER BY r.useful_votes DESC LIMIT 7;";
		//String query = "SELECT * FROM review,user where review.business_id = ? and review.user_id=user.id order by user.useful_votes desc LIMIT 7";
		
		PreparedStatement prep = conn.prepareStatement(query);
		prep.setString(1, businessID);
		ResultSet rs = prep.executeQuery();
		
		List<ReviewObject> result2 = new ArrayList<ReviewObject>();
		
		while (rs.next()) {
			ReviewObject robj = new ReviewObject();
			robj.setId(rs.getString("user_id"));
			robj.setName(rs.getString("name"));
			robj.setStars(rs.getInt("stars"));
			robj.setText(rs.getString("text"));
			
			result2.add(robj);
		}
		rs.close();
		
		return result2;		
			
		
	}

	/**
	 * This function is called for query 3
	 * 
	 * Find the average star rating across all reviews written by a particular user.
	 * 
	 * @input userID
	 * @output One columns - the average star rating.
	 * 
	 * @return the average star rating
	 * @throws SQLException
	 */
	@Override
	public double query3(String userID) throws SQLException {
		// Your code goes here.
		String query = "SELECT avg(stars) as avg_stars FROM review GROUP BY user_id HAVING user_id = ?";
		
		PreparedStatement prep = conn.prepareStatement(query);
		prep.setString(1, userID);
		ResultSet rs = prep.executeQuery();
		
		double result3 = 0.0;
		while (rs.next()){
		result3 = rs.getDouble("avg_stars");
		
		}
		rs.close();
		return result3;			
		
	}

	/**
	 * This function is called for query 4
	 * 
	 * Get the businesses in Providence, RI that have been reviewed by more than 5 'elite' users. 
	 * Users who have written more than 10 reviews are called 'elite' users. 
	 * Results should be ordered by the 'elite' user count in descending order. Return top 7 businesses.
	 * 
	 * @input N/A
	 * @output Seven columns - the business id, business name, business full address, review count, photo url, stars, and the count of the 'elite' users for the particular business.
	 * 
	 * @return A List of BusinessObject representing the results to the query.
	 * @throws SQLException
	 */
	@Override
	public List<BusinessObject> query4() throws SQLException {
		// Your code goes here. Refer to BusinessObject.java
		// FOR FULL CREDIT make sure to set the id, name, address, reviewCount, photoUrl, stars, and elite count properties of your BusinessObjects
		
		String PartA = "SELECT * FROM user WHERE review_count>=10";
		
		
		String query = "with PartA as ( select id from user where review_count > 10) , "
				+ "part1 as ( select * from business as b join review as r on b.id=r.business_id "
				+ "where city='Providence' and state='RI' and r.user_id in (select id from PartA) ) , "
				+ "part2 as ( select *, count(user_id) as elite_count from part1 group by name ) "
				+ "select * from part2 where elite_count>=5 order by elite_count desc, name desc limit 7";
		

		PreparedStatement prep = conn.prepareStatement(query);
		ResultSet rs = prep.executeQuery();
		
		List<BusinessObject> result4 = new ArrayList<BusinessObject>();
		
		while (rs.next()) {
			BusinessObject obj = new BusinessObject();
			obj.setId(rs.getString("id"));
			obj.setName(rs.getString("name"));
			obj.setAddress(rs.getString("full_address"));
			obj.setReviewCount(rs.getInt("review_count"));
			obj.setPhotoUrl(rs.getString("photo_url"));
			obj.setStars(rs.getDouble("stars"));
			obj.setEliteCount(rs.getInt("elite_count"));
			result4.add(obj);
		}
		rs.close();
		
		return result4;		
		
	}

	/**
	 * This function is called for query 5
	 * 
	 * Get the businesses in Providence, RI that have the highest percentage of five star reviews, and have been reviewed at least 20 times.
	 * Results should be ordered by the percentage in descending order. Return top 7 businesses.
	 * 
	 * @input N/A
	 * @output Seven columns - the business id, business name, business full address, review count, photo url, stars, and percentage of five star reviews
	 * 
	 * @return A List of BusinessObject representing the results to the query.
	 * @throws SQLException
	 */
	@Override
	public List<BusinessObject> query5() throws SQLException {
		// Your code goes here. Refer to BusinessObject.java
		// FOR FULL CREDIT make sure to set the id, name, address, reviewCount, photoUrl, stars, and percentage properties of your BusinessObjects
		
		String query = "with tt as (select business_id, count(stars) as total_stars from review group by business_id ), "
				+ "fv as ( select *, count(stars) as five_stars from (select * from review where stars =5) group by business_id) , "
				+ "ct as ( select *, five_stars*1.0/(total_stars*1.0) as percentage from tt join fv on tt.business_id = fv.business_id ) "
				+ "select b.id, b.name, b.full_address, b.review_count, b.photo_url, b.stars, ct.percentage "
				+ "from ct join business as b on ct.business_id = b.id "
				+ "where city = 'Providence' and state = 'RI' and review_count>=20 order by percentage desc limit 7";
		
		PreparedStatement prep = conn.prepareStatement(query);
		ResultSet rs = prep.executeQuery();
		
		List<BusinessObject> result5 = new ArrayList<BusinessObject>();
		
		while (rs.next()) {
			BusinessObject obj = new BusinessObject();
			obj.setId(rs.getString("id"));
			obj.setName(rs.getString("name"));
			obj.setAddress(rs.getString("full_address"));
			obj.setReviewCount(rs.getInt("review_count"));
			obj.setPhotoUrl(rs.getString("photo_url"));
			obj.setStars(rs.getDouble("stars"));
			obj.setPercentage(rs.getDouble("percentage"));
			result5.add(obj);
		}
		rs.close();
		
		return result5;		
		
	}
}