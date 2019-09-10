package yelp;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Cursor;
import java.awt.Desktop;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.net.URI;
import java.text.DecimalFormat;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.border.Border;

/**
 * @author akurihar, rchhay
 */
public class ReviewItem extends JPanel {
	
	private JLabel _nameLabel, _starLabel;
	private JButton _avgStarsButton;
	private ReviewObject _model;
	private JTextArea _textArea;
	private MainPanel _mainPanel;
	
	public ReviewItem(MainPanel mainPanel) {
		super();
		
		_mainPanel = mainPanel;
		
		// The default model is a placeholder when query 2 has not been implemented.
		ReviewObject defaultModel = new ReviewObject();
		defaultModel.setId(null);
		defaultModel.setName("CS127 Student");
		defaultModel.setStars(0);
		defaultModel.setText("Implement Query 2 to see reviews!");
		_model = defaultModel;
		
		this.setPreferredSize(new Dimension(500,100));
		this.setBackground(Color.WHITE);
		this.setBorder(BorderFactory.createLineBorder(new Color(221, 221, 221)));
		this.setLayout(new BorderLayout());
		
		JPanel userPanel = new JPanel();
		userPanel.setPreferredSize(new Dimension(150, 100));
		userPanel.setBackground(new Color(0,0,0,0));	
		userPanel.setLayout(new GridLayout(3,1));
		
		_nameLabel = new JLabel(_model.getName(), JLabel.CENTER);
		_nameLabel.setFont(new Font("SansSerif", Font.BOLD, 14));
		_nameLabel.setCursor(new Cursor(Cursor.HAND_CURSOR));	
        _nameLabel.addMouseListener(new userNameListener());
		
		_avgStarsButton = new JButton("User Avg.");
		_avgStarsButton.setSize(new Dimension(145,45));
		_avgStarsButton.addActionListener(new ButtonListener(this));
		
		JPanel avgStarsPanel = new JPanel();
		avgStarsPanel.setPreferredSize(new Dimension(150,100));
		avgStarsPanel.setBackground(new Color(0,0,0,0));
		avgStarsPanel.add(_avgStarsButton);
		
		_starLabel = new JLabel("Stars: " + _model.getStars(), JLabel.CENTER);
		
		userPanel.add(_nameLabel);
		userPanel.add(avgStarsPanel);
		userPanel.add(_starLabel);
		
		_textArea = new JTextArea(_model.getText());
		_textArea.setLineWrap(true);
		_textArea.setWrapStyleWord(true);
		_textArea.setMargin(new Insets(5,5,5,5));
		_textArea.setEditable(false);
		
		JScrollPane scrollPane = new JScrollPane(_textArea);
		scrollPane.setBackground(Color.WHITE);
		scrollPane.setBorder(BorderFactory.createEmptyBorder(5,5,5,5));
		
		this.add(userPanel, BorderLayout.WEST);
		this.add(scrollPane, BorderLayout.CENTER);		
	}
	
	public void setModel(ReviewObject model) {
		_model = model;
		_nameLabel.setText("");
		_nameLabel.setText(model.getName());
		_starLabel.setText("Stars: " + model.getStars());
		_textArea.setText(model.getText());
		_textArea.setCaretPosition(0);
		this.repaint();
	}
	
	private class ButtonListener implements ActionListener {
		
		private ReviewItem _reviewItem;
		
		public ButtonListener(ReviewItem reviewItem) {
			_reviewItem = reviewItem;
		}
		
		public void actionPerformed(ActionEvent arg0) {
			JLabel label = new JLabel("Implement Query 3 for User Avg.", JLabel.CENTER);
			double userAverage = _mainPanel.getUserAvgStars(_model.getId());
			if (userAverage >= 0) {
				DecimalFormat df = new DecimalFormat("#.##");
				label.setText("User Average Stars: " + df.format(userAverage));
				JOptionPane.showMessageDialog(_reviewItem, label, "", JOptionPane.PLAIN_MESSAGE);
			}
		}
	}
	
	private class userNameListener extends MouseAdapter {
    	
    	@Override
        public void mouseClicked(MouseEvent e) {
    		if (_model.getId() == null) return;
            try {
                Desktop.getDesktop().browse(new URI("http://www.yelp.com/user_details?userid=" + _model.getId()));
            } catch (Exception ex) {
            	System.out.println("This functionality is not supported in your platform.");
            }
    	}
	}
}
