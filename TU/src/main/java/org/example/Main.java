package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Properties;

public class Main {
    private static final String CONNECTION_STRING = "jdbc:mysql://localhost:3306/";
    private static final String DATABASE_NAME = "exam";

    private static Connection connection;
    private static String QUERY;
    private static PreparedStatement statement;

    private static BufferedReader reader;

    public static void main(String[] args) throws SQLException, IOException {

        reader = new BufferedReader(new InputStreamReader(System.in));

        Properties props = new Properties();

        props.setProperty("user", "root");
        props.setProperty("password", "1234");

        connection = DriverManager.getConnection(CONNECTION_STRING + DATABASE_NAME, props);

        // 1. Create Table By Given Name and Columns - Example:
        // create table `product`(`id` int AUTO_INCREMENT primary key, `name` varchar(20), `description` varchar(20), `price` decimal(8,3), `category_id` int);
        // create table `category` (`id` int AUTO_INCREMENT NOT NULL primary key, `name` varchar(20));

//        String line = reader.readLine();
//        String tableName = line.substring(line.indexOf("`"), line.indexOf("("));
//        String columns = line.substring(line.indexOf("("), line.lastIndexOf(")") + 2);
//
//        createTable(tableName, columns);

        // 2. Drop Table By Name

//        String table = reader.readLine();
//
//        dropTable(table);

        // 3. List All Tables

        //listAllTables();

        // 4. Get Table Info

//        String table = reader.readLine();
//
//        getTableInfo(table);

        // 5. Select Products

        //selectProducts();

        // 6. Insert Data

//        insertDataIntoCategoryTable();
//        insertDataIntoProductTable();

        // 7. Delelte Products in a price range

//        Integer bottom = Integer.parseInt(reader.readLine());
//        Integer top = Integer.parseInt(reader.readLine());
//
//        deleteProductsWherePriceIsBetween(bottom, top);

        // 8. Create Index
        // Example: CreateIndex SampleId ON product (id)

//        String line = reader.readLine();
//
//        String indexName = line.substring(line.indexOf(" ") + 1, line.indexOf("ON") - 1);
//        String tableName = line.substring(line.indexOf("ON") + 3, line.indexOf("(") - 1);
//        String columnName = line.substring(line.indexOf("(") + 1, line.indexOf(")"));
//
//        createIndex(indexName, tableName, columnName);

        // 9. Drop Index

        String index = reader.readLine();
        String table = reader.readLine();

        dropIndex(index, table);
    }

    private static void createTable(String tableName, String columns) {
        String query = "create table " + tableName + columns;

        try{
            statement = connection.prepareStatement(query);

            statement.execute();
            statement.close();
            connection.close();

            System.out.println("Table created successfully.");
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private static void dropTable(String tableName) {
        String query = "drop table " + tableName;

        try{
            statement = connection.prepareStatement(query);

            statement.execute();
            statement.close();
            connection.close();

            System.out.println("Table dropped successfully.");
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private static void listAllTables() {
        try{
            DatabaseMetaData md = connection.getMetaData();
            ResultSet rs = md.getTables(DATABASE_NAME, null, "%", null);

            while (rs.next()) {
                System.out.println(rs.getString(3));
            }

            connection.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private static void getTableInfo(String tableName) {
        try {
            DatabaseMetaData mtdt = connection.getMetaData();

            ResultSet rs = mtdt.getTables(DATABASE_NAME, "%", tableName, null);

            ResultSetMetaData rsmd = rs.getMetaData();
            int numCols = rsmd.getColumnCount();
            for (int i = 1; i <= numCols; i++) {
                if (i > 1)
                    System.out.print(", ");
                System.out.print(rsmd.getColumnLabel(i));
            }
            System.out.println("");
            while (rs.next()) {
                for (int i = 1; i <= numCols; i++) {
                    if (i > 1)
                        System.out.print(", ");
                    System.out.print(rs.getString(i));
                }
                System.out.println("");
            }
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void selectProducts() {
        QUERY = "SELECT p.`name` FROM `product` AS `P`\n" +
                "JOIN `category` AS `c` ON p.`category_id` = c.`id`\n" +
                "WHERE p.`price` > 100 and c.`id` between 1 and 10\n" +
                "ORDER BY p.`price` DESC;";

        try {
            statement = connection.prepareStatement(QUERY);

            ResultSet resultSet = statement.executeQuery();

            int count = 0;

            while (resultSet.next()) {
                count++;
                System.out.printf("%d. %s%n", count, resultSet.getString("name"));
            }

            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertDataIntoCategoryTable() {
        QUERY = "INSERT INTO `category`(`id`, `name`) \n" +
                "VALUES (1, 'MALE CLOTHES'), (2, 'FEMALE CLOTHES');";
        try {
            statement = connection.prepareStatement(QUERY);

            statement.execute();
            statement.close();

            System.out.println("Data into category table has been inserted successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertDataIntoProductTable() {
        QUERY =  "INSERT INTO `product` (`id`, `name`, `description`, `price`, `category_id`) \n" +
                "VALUES (1, 'Product 1', 'Long descriptions', 100, 1), (2, 'Product 2', 'Description ....', 200, 2), (3, 'Product 3', 'Description ....', 250, 2);";
        try {
            statement = connection.prepareStatement(QUERY);

            statement.execute();
            statement.close();

            System.out.println("Data into product table has been inserted successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void deleteProductsWherePriceIsBetween(Integer bottom, Integer top) {
        QUERY = "DELETE FROM `product`\n" +
                "WHERE `price` between ? and ?;";

        try {
            statement = connection.prepareStatement(QUERY);
            statement.setInt(1, bottom);
            statement.setInt(2, top);

            statement.execute();
            statement.close();

            System.out.println("Products deleted");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createIndex(String indexName, String tableName, String columnName) {
        QUERY = "CREATE INDEX " + indexName + " ON " + tableName + " (" + columnName + ");";

        try {
            statement = connection.prepareStatement(QUERY);

            statement.execute();
            statement.close();

            System.out.println("Index created");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void dropIndex(String indexName, String tableName) {
        QUERY = "DROP INDEX " + indexName + " ON " + tableName;

        try {
            statement = connection.prepareStatement(QUERY);

            statement.execute();
            statement.close();

            System.out.println("Index deleted");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
