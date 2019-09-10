package yelp;
import javax.swing.JFrame;
import javax.swing.UIManager;
import javax.swing.plaf.ColorUIResource;

/**
 * @author akurihar, rchhay
 */
public class App extends JFrame {

	public App() {
		super("Yelp! Desktop Client");
		this.setDefaultCloseOperation(EXIT_ON_CLOSE);
		
		MainPanel mainPanel = new MainPanel();
		this.add(mainPanel);
		
		// Make dialog backgrounds white
		UIManager UI = new UIManager();
		UI.put("OptionPane.background",new ColorUIResource(255,255,255));
		UI.put("Panel.background",new ColorUIResource(255,255,255));
		
		this.pack();
		this.setVisible(true);
	}

	public static void main(String[] argv) {
		new App();
	}

}
