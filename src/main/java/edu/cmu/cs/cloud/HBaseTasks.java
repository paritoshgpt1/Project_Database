package edu.cmu.cs.cloud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

public class HBaseTasks {

    /**
     * The private IP address(es) of HBase zookeeper nodes.
     */
    private static String zkPrivateIPs = "10.0.1.14,10.0.1.18,10.0.1.13";
    /**
     * The name of your HBase table.
     */
    private static TableName tableName = TableName.valueOf("business");
    /**
     * HTable handler.
     */
    private static Table bizTable;
    /**
     * HBase connection.
     */
    private static Connection conn;
    /**
     * Byte representation of column family.
     */
    private static byte[] bColFamily = Bytes.toBytes("data");
    /**
     * Logger.
     */
    private static final Logger LOGGER = Logger.getRootLogger();

    /**
     * Initialize HBase connection.
     *
     * @throws IOException if a network exception occurs.
     */
    private static void initializeConnection() throws IOException {
        // Turn of the logging to get rid of unnecessary standard output.
        LOGGER.setLevel(Level.OFF);
        if (!zkPrivateIPs.matches("\\d+.\\d+.\\d+.\\d+(,\\d+.\\d+.\\d+.\\d+)*")) {
            System.out.print("Malformed HBase IP address");
            System.exit(-1);
        }
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkPrivateIPs);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        conf.set("hbase.cluster.distributed", "true");
        conf.set("zookeeper.znode.parent","/hbase-unsecure");
        conn = ConnectionFactory.createConnection(conf);
        bizTable = conn.getTable(tableName);
    }

    /**
     * Clean up resources.
     *
     * @throws IOException
     * Throw IOEXception
     */
    private static void cleanup() throws IOException {
        if (bizTable != null) {
            bizTable.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * You should complete the missing parts in the following method.
     * Feel free to add helper functions if necessary.
     *
     * For all questions, output your answer in ONE single line,
     * i.e. use System.out.print().
     *
     * @param args The arguments for main method.
     * @throws IOException if a remote or network exception occurs.
     */
    public static void main(String[] args) throws IOException {
        initializeConnection();
        switch (args[0]) {
            case "demo":
                demo();
                break;
            case "q12":
                q12();
                break;
            case "q13":
                q13();
                break;
            case "q14":
                q14();
                break;
            case "q15":
                q15();
                break;
            default:
                break;
        }
        cleanup();
    }

    /**
     * This is a demo of how to use HBase Java API.
     * It will print the number of businesses in "Pittsburgh".
     *
     * @throws IOException if a remote or network exception occurs.
     */
    private static void demo() throws IOException {
        Scan scan = new Scan();
        byte[] bCol = Bytes.toBytes("city");
        scan.addColumn(bColFamily, bCol);
        SubstringComparator comp = new SubstringComparator("Pittsburgh");
        Filter filter = new SingleColumnValueFilter(
                bColFamily, bCol, CompareFilter.CompareOp.EQUAL, comp);
        scan.setFilter(filter);
        ResultScanner rs = bizTable.getScanner(scan);
        int count = 0;
        for (Result r = rs.next(); r != null; r = rs.next()) {
            count++;
        }
        System.out.println("Scan finished. " + count + " match(es) found.");
        rs.close();
    }

    /**
     * Question 12.
     *
     * Scenario:
     * What's that new "Asian Fusion" place in "Shadyside" with free wifi and
     * bike parking?
     *
     * Print each name of the business on a single line.
     * If there are multiple answers, print all of them.
     *
     * Note:
     * 1. The "neighborhood" column should contain "Shadyside" as a substring.
     * 2. The "categories" column should contain "Asian Fusion" as a substring.
     * 3. The "WiFi" and "BikeParking" information can be found in the
     * "attributes" column. Please be careful about the format of the data.
     *
     * You are allowed to make changes such as modifying method name, parameter
     * list and/or return type.
     */
    private static void q12() throws IOException{
        Scan scan = new Scan();
        byte[] name = Bytes.toBytes("name");
        scan.addColumn(bColFamily, name);

        Filter filter1 = createFilter("neighborhood", scan, "SubstringComparator",
                "Shadyside", "equal");
        Filter filter2 = createFilter("categories", scan, "SubstringComparator",
                "Asian Fusion", "equal");
        Filter filter3 = createFilter("attributes", scan, "RegexStringComparator",
                "'WiFi': 'free'", "equal");
        Filter filter4 = createFilter("attributes", scan, "RegexStringComparator",
                "'BikeParking': True", "equal");

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);
        filterList.addFilter(filter3);
        filterList.addFilter(filter4);
        scan.setFilter(filterList);
        ResultScanner rs = bizTable.getScanner(scan);
        for (Result r : rs) {
            printValue(r, "name");
        }
        rs.close();
    }

    private static Filter createFilter(String colName, Scan scan, String comparatorType, String searchString,
                                       String comparisonType) {
        byte[] column = Bytes.toBytes(colName);
        scan.addColumn(bColFamily, column);
        CompareFilter.CompareOp compareFilter = null;
        switch (comparisonType) {
            case "equal":
                compareFilter = CompareFilter.CompareOp.EQUAL;
                break;
            case "not_equal":
                compareFilter = CompareFilter.CompareOp.NOT_EQUAL;
                break;
            case "greater":
                compareFilter = CompareFilter.CompareOp.GREATER_OR_EQUAL;
                break;
        }
        switch (comparatorType) {
            case "SubstringComparator": {
                SubstringComparator comp = new SubstringComparator(searchString);
                return new SingleColumnValueFilter(bColFamily, column, compareFilter, comp);
            }
            case "RegexStringComparator": {
                RegexStringComparator comp = new RegexStringComparator(searchString);
                return new SingleColumnValueFilter(bColFamily, column, compareFilter, comp);
            }
            case "BinaryComparator": {
                BinaryComparator comp = new BinaryComparator(Bytes.toBytes(Integer.parseInt(searchString)));
                return new SingleColumnValueFilter(bColFamily, column, compareFilter, comp);
            }
        }
        return null;
    }

    private static void printValue(Result r, String colName) {
        byte[] col = Bytes.toBytes(colName);
        byte[] value = r.getValue(bColFamily, col);
        String readableVal = Bytes.toString(value);
        System.out.println(readableVal);
    }

    /**
     * Question 13.
     *
     * Scenario:
     * I'm looking for some Indian food to eat in Downtown or Oakland of Pittsburgh
     * that start serving on Fridays at 5pm, but still deliver in case I'm too lazy
     * to drive there.
     *
     * Print each name of the business on a single line.
     * If there are multiple answers, print all of them.
     *
     * Note:
     * 1. The "name" column should contain "India" as a substring.
     * 2. The "neighborhood" column should contain "Downtown" or "Oakland"
     * as a substring.
     * 3. The "city" column should contain "Pittsburgh" as a substring.
     * 4. The "hours" column shows the hours when businesses start serving.
     * 5. The "RestaurantsDelivery" information can be found in the
     * "attributes" column.
     *
     * Hint:
     * You may consider using other comparators in the filter.
     *
     * You are allowed to make changes such as modifying method name, parameter
     * list and/or return type.
     */
    private static void q13() throws IOException{
        Scan scan = new Scan();

        Filter filter1 = createFilter("name", scan, "SubstringComparator",
                "India", "equal");
        Filter filter2 = createFilter("neighborhood", scan, "RegexStringComparator",
                "Downtown|Oakland", "equal");
        Filter filter3 = createFilter("city", scan, "SubstringComparator",
                "Pittsburgh", "equal");
        Filter filter4 = createFilter("hours", scan, "RegexStringComparator",
                "'Friday': '17:00", "equal");
        Filter filter5 = createFilter("attributes", scan, "RegexStringComparator",
                "'RestaurantsDelivery': True", "equal");
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);
        filterList.addFilter(filter3);
        filterList.addFilter(filter4);
        filterList.addFilter(filter5);
        scan.setFilter(filterList);
        ResultScanner rs = bizTable.getScanner(scan);
        for (Result r : rs) {
            printValue(r, "name");
        }
        rs.close();

    }

    /**
     * Question 14.
     *
     * Write HBase query to do the equivalent of the SQL query:
     * SELECT name FROM businesses where business_id = "I1vE5o98Wy5pCULJoEclqw"
     *
     * Hint:
     * You may consider using other HBase operations which are used to search
     * and retrieve one single row by the rowkey.
     *
     * You are allowed to make changes such as modifying method name, parameter
     * list and/or return type.
     */
    private static void q14() throws IOException{
        Get g = new Get(Bytes.toBytes("I1vE5o98Wy5pCULJoEclqw"));
        Result result = bizTable.get(g);
        printValue(result, "name");
    }

    /**
     * Question 15.
     *
     * Write HBase query to do the equivalent of the SQL query:
     * SELECT COUNT(*) FROM businesses
     *
     * Print the number on a single line.
     *
     * Note:
     * 1. HBase uses Coprocessor to perform data aggregation across multiple
     * region servers, you need to enable Coprocessors inside HBase shell
     * before writing Java code.
     *
     *   Step 1. disable the table
     *   hbase> disable 'mytable'
     *
     *   Step 2. add the coprocessor
     *   hbase> alter 'mytable', METHOD =>
     *     'table_att','coprocessor'=>
     *     '|org.apache.hadoop.hbase.coprocessor.AggregateImplementation||'
     *
     *   Step 3. re-enable the table
     *   hbase> enable 'mytable'
     *
     * 2. You may want to look at the AggregationClient Class in HBase APIs.
     *
     * You are allowed to make changes such as modifying method name, parameter
     * list and/or return type.
     */
    private static void q15() {

    }

}