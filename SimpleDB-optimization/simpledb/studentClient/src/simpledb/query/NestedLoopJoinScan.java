package simpledb.query;

public class NestedLoopJoinScan implements Scan {

    private Scan scan1;
    private Scan scan2;
    private String fldname1, fldname2;

    public NestedLoopJoinScan(Scan scan1, Scan scan2, String fldname1, String fldname2) {
        this.scan1 = scan1;
        this.scan2 = scan2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        scan1.beforeFirst();
        scan2.beforeFirst();
        scan1.next();
    }

    @Override
    public boolean next() {
        // TODO: Get the next tuple using the nested loop join algorithm
        return false;
    }

    @Override
    public void close() {
        scan1.close();
        scan2.close();
    }

    @Override
    public Constant getVal(String fldname) {
        if (scan1.hasField(fldname))
            return scan1.getVal(fldname);
        else
            return scan2.getVal(fldname);
    }

    @Override
    public int getInt(String fldname) {
        if (scan1.hasField(fldname))
            return scan1.getInt(fldname);
        else
            return scan2.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        if (scan1.hasField(fldname))
            return scan1.getString(fldname);
        else
            return scan2.getString(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return scan1.hasField(fldname) || scan2.hasField(fldname);
    }
}
