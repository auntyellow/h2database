/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.Statement;
import java.util.BitSet;

import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Tests H2 TCP server protocol with `lazy_query_execution`.
 */
public class TestLazyResultRemote extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.networked = true;
        test.testFromMain();
    }

    @Override
    public boolean isEnabled() {
        return config.networked;
    }

    @Override
    public void test() throws Exception {
        deleteDb("test");
        testException(false);
        testException(true);
        testMultiStatements(false);
        testMultiStatements(true);
        deleteDb("test");
    }

    private void testException(boolean lazy) throws Exception {
        for (int fetchSize = 2; fetchSize <= 7; fetchSize ++) {
            try (
                Connection conn = getConnection("test");
                Statement stmt = conn.createStatement();
            ) {
                stmt.setFetchSize(fetchSize);
                stmt.execute("SET LAZY_QUERY_EXECUTION " + lazy);
                stmt.execute("DROP TABLE IF EXISTS test");
                stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, x1 VARCHAR)");
                stmt.execute("INSERT INTO test (id, x1) VALUES (1, '2'), (2, '3'), (3, '4')");
                // fetchSize = 2: COMMAND_EXECUTE_QUERY(2) + RESULT_FETCH_ROWS(1)
                // fetchSize = 3: COMMAND_EXECUTE_QUERY(3) + RESULT_FETCH_ROWS(0)
                // fetchSize >= 4: COMMAND_EXECUTE_QUERY(3)
                int rowCount = 0;
                int idSum = 0;
                StringBuilder x1Concat = new StringBuilder();
                try (ResultSet rs = stmt.executeQuery("SELECT id, x1 FROM test")) {
                    while (rs.next()) {
                        rowCount ++;
                        idSum += rs.getInt("id");
                        x1Concat.append(rs.getString("x1"));
                    }
                }
                assertEquals(3, rowCount);
                assertEquals(6, idSum);
                assertEquals("234", x1Concat.toString());

                stmt.execute("INSERT INTO test (id, x1) VALUES (4, '5'), (5, '6'), (6, '7'), (7, 'x')");
                // fetchSize = 2: COMMAND_EXECUTE_QUERY(2) + RESULT_FETCH_ROWS(2 + 2 + x) => rowCount = 6
                // fetchSize = 3: COMMAND_EXECUTE_QUERY(3) + RESULT_FETCH_ROWS(3 + x) => rowCount = 6
                // fetchSize = 4: COMMAND_EXECUTE_QUERY(4) + RESULT_FETCH_ROWS(2x) => rowCount = 4
                // fetchSize = 5: COMMAND_EXECUTE_QUERY(5) + RESULT_FETCH_ROWS(1x) => rowCount = 5
                // fetchSize = 6: COMMAND_EXECUTE_QUERY(6) + RESULT_FETCH_ROWS(x) => rowCount = 6
                // fetchSize = 7: COMMAND_EXECUTE_QUERY(6x) => rowCount = 0
                rowCount = 0;
                idSum = 0;
                x1Concat.setLength(0);
                try (ResultSet rs = stmt.executeQuery("SELECT id, CAST(x1 AS INT) x1 FROM test")) {
                    while (rs.next()) {
                        rowCount ++;
                        idSum += rs.getInt("id");
                        x1Concat.append(rs.getString("x1"));
                    }
                    fail();
                } catch (SQLDataException e) {
                    if (!lazy || fetchSize == 7) {
                        assertEquals(0, rowCount);
                        assertEquals(0, idSum);
                        assertEquals("", x1Concat.toString());
                    } else if (fetchSize == 4) {
                        assertEquals(4, rowCount);
                        assertEquals(10, idSum);
                        assertEquals("2345", x1Concat.toString());
                    } else if (fetchSize == 5) {
                        assertEquals(5, rowCount);
                        assertEquals(15, idSum);
                        assertEquals("23456", x1Concat.toString());
                    } else {
                        assertEquals(6, rowCount);
                        assertEquals(21, idSum);
                        assertEquals("234567", x1Concat.toString());
                    }
                }
            }
        }
    }

    private void testMultiStatements(boolean lazy) throws Exception {
        try (
            Connection conn = getConnection("test");
            Statement s1 = conn.createStatement();
            Statement s2 = conn.createStatement();
        ) {
            int count = 300;
            s1.execute("SET LAZY_QUERY_EXECUTION " + lazy);
            s1.execute("DROP TABLE IF EXISTS test");
            s1.execute("CREATE TABLE test AS SELECT * FROM SYSTEM_RANGE(0, " + (count - 1) + ")");
            String query = "TABLE test";
            ResultSet rs1 = s1.executeQuery(query), rs2 = s2.executeQuery(query);
            BitSet bs1 = new BitSet(count), bs2 = new BitSet(count);
            int count2 = 0;
            while (rs1.next() && rs2.next()) {
                bs1.set(rs1.getInt(1));
                bs2.set(rs2.getInt(1));
                count2++;
            }
            assertEquals(count, count2);
            for (int i = 0, b1 = 0, b2 = 0; i < count2; i++, b1++, b2++) {
                b1 = bs1.nextSetBit(b1);
                b2 = bs2.nextSetBit(b2);
                assertEquals(b1, b2);
            }
        }
    }

}
