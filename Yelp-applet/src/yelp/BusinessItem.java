package yelp;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Desktop;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.Image;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URI;
import java.awt.Cursor;

import javax.imageio.ImageIO;
import javax.swing.BorderFactory;
import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.Border;

/**
 * @author akurihar, rchhay
 */
public class BusinessItem extends JPanel {

	private int _index;
	private boolean _selected;
	private MainPanel _mainPanel;
	private BusinessObject _model;
	
	private JLabel _reviewLabel, _starLabel;
	private JLabel _nameLabel, _imageLabel;
	private Image _photo;
	
	public BusinessItem(MainPanel mainPanel, int index) {
		super();
	
		_mainPanel = mainPanel;
		_index = index;
		_selected = false;
		
		// The default model is a placeholder when query 1 has not been implemented.
		BusinessObject defaultModel = new BusinessObject();
		defaultModel.setId(null);
		defaultModel.setName("Implement Query 1");
		defaultModel.setPhotoUrl("http://s3-media2.ak.yelpcdn.com/bphoto/JOWh-4glqDRiw-JYCKW-gA/ms.jpg");
		defaultModel.setReviewCount(0);
		defaultModel.setStars(0);
		_model = defaultModel;
		
		this.setPreferredSize(new Dimension(400, 100));
		this.setBackground(new Color(247, 23, 0));
		this.setLayout(new BorderLayout());
		
		_imageLabel = new JLabel();
		_imageLabel.setBorder(BorderFactory.createEmptyBorder(12,13,12,13));
		this.getImageFromUrl(_model.getPhotoUrl());
		
		JPanel textPanel = new JPanel();
		textPanel.setLayout(new GridLayout(3,1));
		textPanel.setBackground(new Color(0,0,0,0));
		textPanel.setPreferredSize(new Dimension(250,100));
		
		_nameLabel = new JLabel(_model.getName());
		_reviewLabel = new JLabel("Reviews: " + _model.getReviewCount());
		_starLabel = new JLabel("Stars: " + _model.getStars());
		
		_nameLabel.setFont(new Font("SansSerif", Font.BOLD, 14));
		_nameLabel.setForeground(Color.WHITE);
		_reviewLabel.setForeground(Color.WHITE);
		_starLabel.setForeground(Color.WHITE);
		
		_nameLabel.setCursor(new Cursor(Cursor.HAND_CURSOR));	
        _nameLabel.addMouseListener(new businessNameListener());
		
		textPanel.add(_nameLabel);	
		textPanel.add(_reviewLabel);
		textPanel.add(_starLabel);
		
		Border border = BorderFactory.createLineBorder(new Color(196, 18, 0));
		this.setBorder(border);
		
		this.addMouseListener(new businessItemListener());
		this.add(_imageLabel, BorderLayout.WEST);
		this.add(textPanel, BorderLayout.CENTER);
	}
	
	public void setSelected(boolean selected){
		_selected = selected;
		if (selected) {
			this.setBackground(new Color(196, 18, 0));
		} else {
			this.setBackground(new Color(247, 23, 0));
		}
	}
	
	public void setModel(BusinessObject model) {
		_model = model;
		_nameLabel.setText(model.getName());
		_reviewLabel.setText("Reviews: " + model.getReviewCount());
		_starLabel.setText("Stars: " + model.getStars());
		this.getImageFromUrl(_model.getPhotoUrl());
		this.repaint();
	}
	
	public void getImageFromUrl(String url) {
		try {
			Image image = ImageIO.read(new URL(url));
			_photo = image.getScaledInstance(75, 75, Image.SCALE_SMOOTH);
			_imageLabel.setIcon(new ImageIcon(_photo));
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String getBusinessId() {
		return _model.getId();
	}
	
	private class businessItemListener extends MouseAdapter {
		
		@Override
		public void mouseClicked(MouseEvent e) {
			_mainPanel.setSelectedBusinessItem(_index);
		}
	}
	
	private class businessNameListener extends MouseAdapter {
        	
    	@Override
        public void mouseClicked(MouseEvent e) {
    		if (_model.getId() == null) return;
            try {
                Desktop.getDesktop().browse(new URI("http://www.yelp.com/biz/" + _model.getId()));
            } catch (Exception ex) {
                System.out.println("This functionality is not supported in your platform.");
            }
    	}
	}
	
}
