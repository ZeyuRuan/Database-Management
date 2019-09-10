package yelp;
import java.sql.SQLException;
import java.util.List;

/**
 * Author: rchhay
 */

public interface DBController {

	public List<BusinessObject> query1() throws SQLException;
	
	public List<ReviewObject> query2(String businessId) throws SQLException;
	
	public double query3(String userId) throws SQLException;
	
	public List<BusinessObject> query4() throws SQLException;
	
	public List<BusinessObject> query5() throws SQLException;
}
