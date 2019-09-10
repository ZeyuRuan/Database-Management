package bdsim.server.system.concurrency;

import java.util.*;

import bdsim.server.system.BDSystemThread;

/**
 * A graph of lock dependencies to detect cycles.
 * 
 * @author wpijewsk, dclee
 */
public class WaitsForGraph {
	
   //
   // Our internal graph structure
   //
	protected Hashtable<Integer, WaitsForGraph.Node> nodes;

   //
   // For our cycle detection algorithm
   //
   protected final static transient int COLOR_BLACK = 0;
   protected final static transient int COLOR_GREY  = 1;
   protected final static transient int COLOR_WHITE = 2;
   
   //
   // An edge identifier that can be used to try to cut the cycle
   //
   protected WaitsForGraph.Node cut_node;
   
   public WaitsForGraph() {
      this.nodes = new Hashtable<Integer, WaitsForGraph.Node>();
   }
   
	/**
	 * Builds a graph by analyzing a set of locks. You should use this in
	 * production code.
	 * 
	 * @param locks  The locks currently held in the system.
	 */
	public WaitsForGraph(Set<BDTrackableReadWriteLock<BDSystemThread>> locks) {
		this();
		
		//
		// Nasty loops here, old boy
		//
		for (BDTrackableReadWriteLock<BDSystemThread> cur_lock : locks) {
			//
			// Now for each lock, we get the list of thread groups. In each thread group
         // there will be another list of threads that are waiting on another thread group
         // So then we need to loop through these two list of threads, and make an edge in the
         // graph for each unique pair
			//
			for (int i = 0; i < cur_lock.getThreadGroups().size(); i++) {
				Set<BDSystemThread> threads1 = cur_lock.getThreadGroups().get(i).getThreads();
				for (int j = i + 1; j < cur_lock.getThreadGroups().size(); j++) {
               Set<BDSystemThread> threads2 = cur_lock.getThreadGroups().get(j).getThreads();
					//
					// Wow, now that we've done all that, we need to make the nodes
					//
               for (BDSystemThread thread1 : threads1) {
                  for (BDSystemThread thread2 : threads2) {
                     this.addEdge(thread1.getTransactionId(), thread2.getTransactionId());
                  } // FOR
               } // FOR
				} // FOR
			} // FOR
		} // FOR
	}
   
   /**
    * Return a string representation of the internal graph
    * 
    * @return the graph's nodes with edgs
    */
   protected String debugGraph() {
      String ret = "";
      if (!this.nodes.isEmpty()) {
         ret = "DEBUG GRAPH\n" +
               "------------------------------------\n";
         
         Vector<Integer> v = new Vector<Integer>(this.nodes.keySet());
         Collections.sort(v);
         for (Integer idx : v) {
            WaitsForGraph.Node node = this.nodes.get(idx);
            ret += idx + ") " + node + "\n";
         }
         ret += "------------------------------------\n";
      }
      return (ret);
   }
   
   /**
    * Add a new edge from node1 to node2 in the graph
    * 
    * @param node1 the source node
    * @param node2 the sink node
    */
   protected void addEdge(int node1, int node2) {
      //
      // First check whether we have node objects for these two ids
      //
      if (this.nodes.get(node1) == null) {
         this.nodes.put(node1, new WaitsForGraph.Node(node1));
      }
      if (this.nodes.get(node2) == null) {
         this.nodes.put(node2, new WaitsForGraph.Node(node2));
      }
      
      //
      // Now go ahead and add the edges
      //
      this.nodes.get(node1).addEdge(node2);
      //this.nodes.get(node2).addEdge(node1);
   }

	/** 
	 * @return  Whether or not this graph has a cycle
	 */
	public boolean hasCycle() {    
      //
      // First loop through all our nodes and mark each vertex white
      //
      this.cut_node = null;
      for (WaitsForGraph.Node node : this.nodes.values()) {
         if (node != null) node.setColor(WaitsForGraph.COLOR_WHITE);
      }
      //
      // Now traverse the graph, visiting each node and marking it
      //
      for (WaitsForGraph.Node node : this.nodes.values()) {
         if (node != null && node.getColor() == WaitsForGraph.COLOR_WHITE) {
            if (this.visit(node)) return (true);
         }
      } // FOR
      return (false);
   }
   
   /**
    * The second part of determining whether a cycle exists
    * We will set this.cut_node if we have a cycle
    * 
    * @param node the node to begin traversing from and checking all adjacent nodes
    * @return true if a cycle exists
    */
   protected boolean visit(WaitsForGraph.Node node) {
      node.setColor(WaitsForGraph.COLOR_GREY);
      for (Integer idx : node.getEdges()) {
         WaitsForGraph.Node other = this.nodes.get(idx);
         if (other.getColor() == WaitsForGraph.COLOR_GREY) {
            this.cut_node = other;
            return (true);
         } else if (other.getColor() == WaitsForGraph.COLOR_WHITE) {
            if (this.visit(other)) return (true);
         }
      } // FOR
      node.setColor(WaitsForGraph.COLOR_BLACK);
      return (false);
   }
		
	/**
	 * @return  A thread which will break the cycle of dependencies
	 */
	public int getCycleBreaker() {
		return (this.cut_node == null ? 0 : this.cut_node.id);
	}
   
   /**
    * Testing!
    * @param args
    */
   public static void main(String[] args) {
      Vector <Vector<Integer>> graph = new Vector<Vector<Integer>>();
      
      Vector<Integer> group = new Vector<Integer>();
      group.add(1);
      group.add(2);
      graph.add(group);
      
      group = new Vector<Integer>();
      group.add(3);
      group.add(4);
      group.add(5);
      graph.add(group);
      
      group = new Vector<Integer>();
      group.add(6);
      group.add(7);
      group.add(8);
      group.add(1);
      graph.add(group);
      
      WaitsForGraph waitGraph = new WaitsForGraph();
      for (int ctr1 = 0; ctr1 < graph.size(); ctr1++) {
         Vector<Integer> group1 = graph.get(ctr1);
         for (int ctr2 = ctr1 + 1; ctr2 < graph.size(); ctr2++) {
            Vector<Integer> group2 = graph.get(ctr2);
            for (int i = 0; i < group1.size(); i++) {
               for (int j = 0; j < group2.size(); j++) {
                  if (group1.get(i) != group2.get(j)) waitGraph.addEdge(group1.get(i), group2.get(j));
               } // FOR
            } // FOR
         } // FOR
      } // FOR
      System.err.println(waitGraph.debugGraph());
      System.err.println("CYCLE:   " + waitGraph.hasCycle());
      System.err.println("BREAKER: " + waitGraph.getCycleBreaker());
      return;
   }
   
   /**
    * Internal Node Data Structure
    */
   protected class Node {
      protected int id;
      protected Vector<Integer> edges;
      protected int color;

      /**
       * Initialize the node with the proper id
       * @param id
       */
      public Node(int id) {
         this.id = id;
         this.edges = new Vector<Integer>();
         this.color = WaitsForGraph.COLOR_WHITE;
      }
      
      /**
       * Get the list of edges for this node
       * @return
       */
      public Vector<Integer> getEdges() {
         return (this.edges);
      }
      
      /**
       * Adds a directed edge from this node to the other node
       * @param other
       */
      public void addEdge(int other) {
         if (!this.edges.contains(other)) {
            this.edges.add(other);
         }
      }
      
      /**
       * Sets the color
       * This is used for finding cycles
       * @param color
       */
      public void setColor(int color) {
         this.color = color;
      }
      
      /**
       * Returns the color set
       * @return
       */
      public int getColor() {
         return (this.color);
      }
      
      public String toString() {
         String ret = "Node#" + this.id + ": [";
         String add = "";
         for (Integer idx : this.edges) {
            ret += add + Integer.toString(idx);
            add = ", ";
         }
         ret += "]";
         return (ret);
      }
   } // END CLASS
}