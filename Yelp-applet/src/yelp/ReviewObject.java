package yelp;

/**
 * @author: rchhay
 * This class is a model object for Yelp reviews. 
 */
public class ReviewObject {

	private String _id, _name, _text;
	private double _stars;
	
	public ReviewObject() {
		_id = null;
		_name = null;
		_text = null;
		_stars = 0.0;
	}
	
	public void setId(String id) {
		_id = id;
	}
	
	public void setName(String name) {
		_name = name;
	}
	
	public void setText(String text) {
		_text = text;
	}
	
	public void setStars(double stars) {
		_stars = stars;
	}
	
	public String getId() {
		return _id;
	}
	
	public String getName() {
		return _name;
	}
	
	public String getText() {
		return _text;
	}
	
	public double getStars() {
		return _stars;
	}
}
