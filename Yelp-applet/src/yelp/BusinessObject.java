package yelp;

/**
 * @author: rchhay
 * This class is a model object for Yelp businesses. 
 */
public class BusinessObject {
	
	private String _id, _name, _fullAddress, _photoUrl;
	private double _stars, _percentage;
	private int _reviewCount, _eliteCount;

	public BusinessObject(){
		_id = null;
		_name = null;
		_fullAddress = null;
		_photoUrl = null;
		_stars = 0.0;
		_percentage = 0.0;
		_reviewCount = 0;
		_eliteCount = 0;
	}
	
	public void setId(String id) {
		_id = id;
	}
	
	public void setName(String name) {
		_name = name;
	}
	
	public void setAddress(String address) {
		_fullAddress = address;
	}
	
	public void setPhotoUrl(String photo) {
		_photoUrl = photo;
	}
	
	public void setStars(double stars) {
		_stars = stars;
	}
	
	public void setPercentage(double percentage) {
		_percentage = percentage;
	}
	
	public void setReviewCount(int count) {
		_reviewCount = count;
	}
	
	public void setEliteCount(int eliteCount) {
		_eliteCount = eliteCount;
	}
	
	public String getId() {
		return _id;
	}
	
	public String getName() {
		return _name;
	}
	
	public String getAddress() {
		return _fullAddress;
	}
	
	public String getPhotoUrl() {
		return _photoUrl;
	}
	
	public double getStars() {
		return _stars;
	}
	
	public double getPercentage() {
		return _percentage;
	}
	
	public int getReviewCount() {
		return _reviewCount;
	}
	
	public int getEliteCount() {
		return _eliteCount;
	}
}
