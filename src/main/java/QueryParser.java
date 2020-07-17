package main.java;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.update.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.sf.jsqlparser.util.*;
import java.io.StringReader;
import java.util.*;

public class QueryParser {
    /*
    public static void main(String[] args) throws JSQLParserException {
        Set<String> stmts = new HashSet<String>();
        stmts.add("select emp.empId,emp.Name,emp.Sal,org.Name,org.count from Employee as emp, Organization as org ");
        for(String st : stmts) {
            CCJSqlParserManager parserManager = new CCJSqlParserManager();
            Statement stmt = parserManager.parse(new StringReader(st));

            if (stmt instanceof Select) {
                Select select = (Select) parserManager.parse(new StringReader(st));
                PlainSelect plain = (PlainSelect) select.getSelectBody();
                List selectitems = plain.getSelectItems();
                for (int i = 0; i < selectitems.size(); i++) {
                    Expression expression = ((SelectExpressionItem) selectitems.get(i)).getExpression();
                    if (expression instanceof Column) {
                        Column col = (Column) expression;
                        System.out.print(col.getTable() + "," + col.getColumnName());

                    } else if (expression instanceof Function) {
                        Function function = (Function) expression;
                        System.out.print(function.getName() + " " + function.getParameters());
                    }
                }
            } else if (stmt instanceof Insert) {
                Insert insert = (Insert) parserManager.parse(new StringReader(st));
                List insertCols = insert.getColumns();
                for (int i = 0; i < insertCols.size(); i++) {
                    System.out.print( insertCols.get(i)+",");
                }
            }
            else if (stmt instanceof Update) {
                Update insert = (Update) parserManager.parse(new StringReader(st));
                List updateCols = insert.getColumns();
                for (int i = 0; i < updateCols.size(); i++) {
                    System.out.print( updateCols.get(i)+",");
                }
            }
            else if (stmt instanceof Delete) {
                Delete detele = (Delete) parserManager.parse(new StringReader(st));
                Expression deleteTables = detele.getWhere();
                System.out.print( deleteTables+",");
            }
        }
    }
*/


    public static void main(String[] args) throws JSQLParserException{
        //String str = "select emp.id,emp.name,emp.salary from employee as emp";
        //String str = "select @@session.tx_read_only";
        //String str = "select @@var";
        String str = "select nextval (\\'question_sequence\\')";
        //String str = "insert into tbl (col1, col2) values (1,2)";
        //String str = "insert into questions (created_at, updated_at, description, title, id) values (?, ?, ?, ?, ?)";
        //String str = "select id from empl";
        //String str = "update user set id=1";
        //String str = "delete from user where age=2";
        Table list = queryParser(str);

        if (list.getTableName() != null){
            System.out.println("Table " + list.getTableName());
            //System.out.println("Columns " + list.getColumns());
            System.out.println("update count " + list.getUpdateCount());
            System.out.println("Read count " + list.getReadCount());
            System.out.println("delete count " + list.getDeleteCount());
            System.out.println("create count " + list.getCreateCount());
        }else {
            log.debug("Issue with query; table name found null");
        }
    }
    /*
    public static List<DBTable> queryParser(String str) throws JSQLParserException {
        Set<String> stmts = new HashSet<String>();
        DBTable dbTable = new DBTable();
        List<String> cols = new ArrayList<>();
        List<DBTable> dbTables = new LinkedList<>();
        stmts.add(str);
        for(String st : stmts) {
            CCJSqlParserManager parserManager = new CCJSqlParserManager();
            Statement stmt = parserManager.parse(new StringReader(st));
            if (stmt instanceof Select) {
                Select select = (Select) parserManager.parse(new StringReader(st));
                PlainSelect plain = (PlainSelect) select.getSelectBody();
                List selectitems = plain.getSelectItems();
                for (int i = 0; i < selectitems.size(); i++) {
                    Expression expression = ((SelectExpressionItem) selectitems.get(i)).getExpression();
                    if (expression instanceof Column) {
                        Column col = (Column) expression;
                        dbTable.setTableName(col.getTable().toString());
                        cols.add(col.getColumnName());
                        System.out.print(col.getTable() + "," + col.getColumnName());

                    } else if (expression instanceof Function) {
                        Function function = (Function) expression;
                        System.out.print(function.getName() + " " + function.getParameters());
                    }
                }
                dbTable.setColumns(cols);
                dbTables.add(dbTable);
            } else if (stmt instanceof Insert) {
                Insert insert = (Insert) parserManager.parse(new StringReader(st));
                List insertCols = insert.getColumns();
                for (int i = 0; i < insertCols.size(); i++) {
                    System.out.print( insertCols.get(i)+",");
                }
            }
            else if (stmt instanceof Update) {
                Update insert = (Update) parserManager.parse(new StringReader(st));
                List updateCols = insert.getColumns();
                for (int i = 0; i < updateCols.size(); i++) {
                    System.out.print( updateCols.get(i)+",");
                }
            }
            else if (stmt instanceof Delete) {
                Delete detele = (Delete) parserManager.parse(new StringReader(st));
                Expression deleteTables = detele.getWhere();
                System.out.print( deleteTables+",");
            }
        }
        return dbTables;
    }
    */
/*
    public static DBTable queryParser(String str) throws JSQLParserException {
        Set<String> stmts = new HashSet<String>();
        DBTable dbTable = new DBTable();
        List<String> cols = new ArrayList<>();
        //List<DBTable> dbTables = new LinkedList<>();
        Long createCount = 0L;
        Long readCount = 0L;
        Long updateCount = 0L;
        Long deleteCount = 0L;
        stmts.add(str);
        for(String st : stmts) {
            CCJSqlParserManager parserManager = new CCJSqlParserManager();
            Statement stmt = parserManager.parse(new StringReader(st));
            if (stmt instanceof Select) {
                readCount++;
                Select select = (Select) parserManager.parse(new StringReader(st));
                PlainSelect plain = (PlainSelect) select.getSelectBody();
                List selectitems = plain.getSelectItems();
                for (int i = 0; i < selectitems.size(); i++) {
                    Expression expression = ((SelectExpressionItem) selectitems.get(i)).getExpression();
                    if (expression instanceof Column) {
                        Column col = (Column) expression;
                        dbTable.setTableName(col.getTable().toString());
                        cols.add(col.getColumnName());
                    } else if (expression instanceof Function) {
                        Function function = (Function) expression;
                        System.out.print(function.getName() + " " + function.getParameters());
                    }
                }
                //System.out.print("Read(Select) count is " + readCount);
                dbTable.setColumns(cols);
                dbTable.setReadCount(readCount);
            }
            else if (stmt instanceof Insert) {
                Insert insert = (Insert) parserManager.parse(new StringReader(st));
                List insertCols = insert.getColumns();
                for (int i = 0; i < insertCols.size(); i++) {
                    System.out.print( insertCols.get(i)+",");
                }
            }
            else if (stmt instanceof Update) {
                updateCount++;
                Update insert = (Update) parserManager.parse(new StringReader(st));
                List updateCols = insert.getColumns();
                for (int i = 0; i < updateCols.size(); i++) {
                    cols.add(updateCols.get(i).toString());

                   // System.out.print( updateCols.get(i)+",");
                }
                dbTable.setTableName(insert.getTable().getName());
                dbTable.setUpdateCount(updateCount);
                dbTable.setColumns(cols);
            }
            else if (stmt instanceof Delete) {
                deleteCount++;
                Delete detele = (Delete) parserManager.parse(new StringReader(st));
                //Expression deleteTables = detele.getWhere();
                //System.out.println( deleteTables+",");
                dbTable.setTableName(detele.getTable().getName());
                //System.out.println( detele.getTable());
            }
        }
        return dbTable;
    }
*/

    private static final CCJSqlParserManager parserManager = new CCJSqlParserManager();
    private static final Logger log = LoggerFactory.getLogger(QueryParser.class);


    public static Table queryParser(String sqlQuery)  {
        Table dbTable = new Table();
        Statement stmt = null;
        try {
            sqlQuery = sqlQuery.replace("\\","");
            log.debug("sqlquery...{}", sqlQuery);
            // As of now to handle * in query , and making the appview readable.
            sqlQuery = sqlQuery.replace("*","AllColumns");
            log.debug("sqlquery...{}", sqlQuery);
            stmt = parserManager.parse(new StringReader(sqlQuery));
            log.debug("sqlquery..stmt..{}", stmt);
            if (stmt instanceof Select) {
                try{
                    parseSelectQuery((Select)stmt,dbTable);
                }catch (Exception e){
                    log.debug("Exception.. {}", e);
                }
                //parseSelectQuery((Select)stmt,dbTable);
            }
            else if (stmt instanceof Insert) {
                parseInsertQuery((Insert)stmt,dbTable);
            }
            else if (stmt instanceof Update) {
                parseUpdateQuery((Update)stmt,dbTable);
            }
            else if (stmt instanceof Delete) {
                parseDeleteQuery((Delete)stmt,dbTable);
            }
            return dbTable;
        } catch (JSQLParserException e) {
            log.error("Exception occured while parsing sql query {} with details,",sqlQuery,e);
            return null;
        }

    }

    private static void parseDeleteQuery(Delete delete, Table dbTable) {
        Expression deleteTables = delete.getWhere();
        //System.out.print( deleteTables+",");
        dbTable.setTableName(delete.getTable().getName());
        dbTable.setColumnMap(new HashMap<>());
        dbTable.setDeleteCount(1);
    }

    private static void parseUpdateQuery(Update update, Table dbTable) {
        Column column = new Column();
        List updateCols = update.getColumns();
        Map<String,String> cols = new HashMap<>();

        for (int i = 0; i < updateCols.size(); i++) {
            cols.put(updateCols.get(i).toString(),"NA");
            column.setColumnName(updateCols.get(i).toString());
        }
        dbTable.setTableName(update.getTables().get(0).getName());
        dbTable.setColumnMap(cols);
        dbTable.setUpdateCount(1);
    }

    private static void parseInsertQuery(Insert insert, Table dbTable) {
        List insertCols = insert.getColumns();
        Map<String,String> cols = new HashMap<>();
        for (int i = 0; i < insertCols.size(); i++) {
            cols.put(insertCols.get(i).toString(),"NA");
        }
        dbTable.setTableName(insert.getTable().getName());
        dbTable.setColumnMap(cols);
        dbTable.setCreateCount(1);
    }

    private static void parseSelectQuery(Select select, Table dbTable) {
        PlainSelect plain = (PlainSelect) select.getSelectBody();
        List selectitems = plain.getSelectItems();
        TablesNamesFinder tmf = new TablesNamesFinder();
        Map<String,String> cols = new HashMap<>();
        List<String> tableList = tmf.getTableList(select);
        for (int i = 0; i < selectitems.size(); i++) {
            Expression expression = ((SelectExpressionItem) selectitems.get(i)).getExpression();
            if (expression instanceof Column) {
                Column col = (Column) expression;
                cols.put(col.getColumnName(),"NA");
            }
        }
        dbTable.setTableName(tableList.get(0));
        dbTable.setColumnMap(cols);
        dbTable.setReadCount(1);
    }

}
