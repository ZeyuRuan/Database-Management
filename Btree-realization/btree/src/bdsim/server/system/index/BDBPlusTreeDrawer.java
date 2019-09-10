package bdsim.server.system.index;

import java.awt.*;
import javax.swing.*;



public class BDBPlusTreeDrawer extends JComponent {

	private static final long serialVersionUID = 1L;
	private BDBPlusTreeIndex _tree;
    
    public BDBPlusTreeDrawer(BDBPlusTreeIndex tree) {
        super();
        _tree = tree;
    }

    public void drawNode(BDBPlusTreeNode node, int hPos, int vPos, double spacing, Graphics2D g2) {
    	
    	double space_factor = Math.pow((2*node.d()+1), spacing);
    	setPreferredSize(new java.awt.Dimension((int)(space_factor*40), (int)(800+50*spacing)));
    	
        if(node.isLeaf()) g2.setPaint(Color.black);
        else g2.setPaint(Color.black);
        for(int i = 0; i < 2*node.d(); i++) {
            g2.drawRect(hPos+(30*(i - 2*node.d()/2)), vPos, 30, 20);
        }
        for(int i = 0; i < node.keyCount(); i++) {
        	g2.drawString(node.getKey(i) + "", hPos+(30*(i - 2*node.d()/2))+2, vPos+15);
        }
        if(!node.isLeaf()) { //time to draw some children!
            for(int i = 0; i < node.childCount(); i++) {
            	
            	int target_hPos = (int)(hPos+(getSize().width/space_factor)*(i - 2*node.d()/2));
            	
                drawNode(node.getChild(i), target_hPos, vPos+50, spacing+1.0, g2);
                
                g2.drawLine(hPos+30*(i - 2*node.d()/2), vPos+20, target_hPos-30*node.d()+15, vPos+50);
            }
            //old factor: hPos+((30*2*node.d()+5)*(i - 2*node.d()/2))
        }
    }
    
    public void paintComponent(Graphics g) {
        Graphics2D g2 = (Graphics2D)g;
        
        g2.setFont(new Font("arial", Font.PLAIN, 12));
        
        g2.setPaint(Color.white);
        g2.fillRect(0, 0, getSize().width, getSize().height);

        if (_tree != null) {
	        BDBPlusTreeNode root = _tree.getRoot();
	        drawNode(root, (getSize().width)/2, 20, 1.0, g2);
        }
    }
}