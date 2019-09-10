package yelp;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JPanel;

/**
 * @author rchhay, akurihar
 *
 */
public class ButtonPanel extends JPanel {
	
	MainPanel _mainPanel;

	public ButtonPanel(MainPanel mainPanel) {
		super();
		
		_mainPanel = mainPanel;
		
		JButton button1 = new JButton("Query 1");
		JButton button2 = new JButton("Query 4");
		JButton button3 = new JButton("Query 5");
		
		button1.addActionListener(new ButtonListener(1));
		button2.addActionListener(new ButtonListener(4));
		button3.addActionListener(new ButtonListener(5));
		
		this.setPreferredSize(new Dimension(800, 50));
		this.setBackground(new Color(255, 109, 95));	
		this.setLayout(new GridLayout(1,3));
		
		JPanel panel1 = new JPanel();
		JPanel panel2 = new JPanel();
		JPanel panel3 = new JPanel();
		
		panel1.add(button1);
		panel2.add(button2);
		panel3.add(button3);	
		
		Dimension size = new Dimension(150, 30);
        button1.setPreferredSize(size);
        button2.setPreferredSize(size);
        button3.setPreferredSize(size);
		
		this.add(panel1);
		this.add(panel2);
		this.add(panel3);		
	}

	private class ButtonListener implements ActionListener {
		private int _queryNumber;
		
		public ButtonListener(int queryNumber) {
			_queryNumber = queryNumber;
		}
		
		@Override
		public void actionPerformed(ActionEvent e) {
			_mainPanel.getBusinessItems(_queryNumber);
		}
	}
	
}
