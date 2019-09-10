package yelp;
import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.Color;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author akurihar, rchhay
 */
public class MainPanel extends JPanel {

	private static int NUM_ITEMS = 7;
	private List<BusinessItem> _businessItemList;
	private List<ReviewItem> _reviewItemList;
	private DBController _controller;
	
	public MainPanel() {
		super();
		
		_businessItemList = new ArrayList<BusinessItem>();
		_reviewItemList = new ArrayList<ReviewItem>();
		
		try {
			_controller = new DBStudentController();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		this.setPreferredSize(new Dimension(900,800));
		this.setLayout(new BorderLayout());
		this.setBackground(new Color(196, 18, 0));
		
		JPanel businessPanel = new JPanel(new GridLayout(NUM_ITEMS,1));
		JPanel reviewPanel = new JPanel(new GridLayout(NUM_ITEMS,1));
		JPanel headerPanel = new JPanel(new BorderLayout());
		
		JLabel businessLabel = new JLabel("Businesses", JLabel.CENTER);
		businessLabel.setFont(new Font("SansSerif", Font.BOLD, 16));
		businessLabel.setPreferredSize(new Dimension(400,35));
		
		JLabel reviewLabel = new JLabel("Reviews", JLabel.CENTER);
		reviewLabel.setFont(new Font("SansSerif", Font.BOLD, 16));
		reviewLabel.setPreferredSize(new Dimension(500,25));
	
		headerPanel.add(businessLabel, BorderLayout.WEST);
		headerPanel.add(reviewLabel, BorderLayout.CENTER);
		
		for(int i=0; i<NUM_ITEMS; i++) {
			BusinessItem businessItem = new BusinessItem(this, i);
			businessPanel.add(businessItem);
			_businessItemList.add(businessItem);
			
			ReviewItem reviewItem = new ReviewItem(this);
			reviewPanel.add(reviewItem);
			_reviewItemList.add(reviewItem);
		}
		
		try {
			List<BusinessObject> results = _controller.query1();	
			for (int i=0; i<NUM_ITEMS; i++) {
				_businessItemList.get(i).setModel(results.get(i));
			}
			
			_businessItemList.get(0).setBackground(new Color(196, 18, 0));
			
			List<ReviewObject> reviews = _controller.query2(results.get(0).getId());
			for (int i=0; i<NUM_ITEMS; i++) {
				_reviewItemList.get(i).setModel(reviews.get(i));
			}
		} catch (Exception e) {
			// Do nothing
		}
		
		this.add(businessPanel, BorderLayout.WEST);
		this.add(reviewPanel, BorderLayout.CENTER);
		this.add(new ButtonPanel(this), BorderLayout.SOUTH);
		this.add(headerPanel, BorderLayout.NORTH);
	}
	
	public void getReviewsForBusinessItem(String businessId) {
		try {
			List<ReviewObject> reviews = _controller.query2(businessId);
			if(reviews == null || reviews.size() != NUM_ITEMS){
				this.displayError("Your Query 2 does not return the correct number of rows");
			}
			else{
				for (int i=0; i<NUM_ITEMS; i++) {
					_reviewItemList.get(i).setModel(reviews.get(i));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (UnsupportedOperationException e) {
			// Do nothing
		}
	}
	
	public void getBusinessItems(int queryNumber) {
		List<BusinessObject> results = null;
		
		try {
			switch(queryNumber) {
				case 1:
					results = _controller.query1();
					break;
				case 4:
					results = _controller.query4();
					break;
				case 5:
					results = _controller.query5();
					break;
			}
			if (results != null && results.size() == NUM_ITEMS) {
				for (int i=0; i<NUM_ITEMS; i++) {
					_businessItemList.get(i).setModel(results.get(i));
				}
				this.setSelectedBusinessItem(0);
			}
			else{
				displayError("Your Query " + queryNumber + " does not return the correct number of rows");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (UnsupportedOperationException e) {
			displayError("Query " + queryNumber + " is not implemented.");
		}
	}
	
	public void setSelectedBusinessItem(int index) {
		BusinessItem currItem;
		for (int i=0; i<NUM_ITEMS; i++) {
			currItem = _businessItemList.get(i);
			if (i != index) {
				currItem.setSelected(false);
			} else {
				currItem.setSelected(true);
				this.getReviewsForBusinessItem(currItem.getBusinessId());
			}
		}
	}
	
	public double getUserAvgStars(String userId) {
		try {
			double avg = _controller.query3(userId);
			return avg;
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (UnsupportedOperationException e) {
			displayError("Query 3 is not implemented.");
		}
		return -1;
	}
	
	public void displayError(String errorMessage){
		JOptionPane.showMessageDialog(this, errorMessage, "SQL ERROR", JOptionPane.ERROR_MESSAGE);
	}
}
