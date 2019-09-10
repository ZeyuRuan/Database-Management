package bdsim.server.system.concurrency;

/**
 * This code sucks, but I was in a hurry
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu> (12/10/2008)
 */
public class BDTATester {

   public static Integer final_result = 0;
   
   /**
    * @param args
    */
   public static void main(String[] args) {
      System.out.println("+ ==========================================");
      System.out.println("+ TIMESTAMP PROTOCOL TESTER");
      System.out.println("+ ==========================================");
      BDTATimestampTester ts_tester = new BDTATimestampTester();
      try {
         ts_tester.run();
      } catch (Exception ex) {
         ex.printStackTrace();
         System.exit(1);
      }
      if (!ts_tester.pass) BDTATester.final_result = 1;
      System.out.println("TEST RESULT: " + (ts_tester.pass ? "PASS" : "FAIL"));
      
      System.out.println("");
      System.out.println("+ ==========================================");
      System.out.println("+ TWO-PHASE LOCKING TESTER");
      System.out.println("+ ==========================================");
      BDTA2PLTester twopl_tester = new BDTA2PLTester();
      try {
         twopl_tester.run();
      } catch (Exception ex) {
         ex.printStackTrace();
         System.exit(1);
      }
      System.out.println("TEST RESULT: " + (twopl_tester.pass ? "PASS" : "FAIL"));
      

   }

}
