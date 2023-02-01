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

        System.out.println("INSERT COMMAND, PLEASE: ");
        String command = reader.readLine();

        while (!command.equals("END")) {
            switch (command) {
                case "CREATE_TABLE":

                    // 1. Create Table By Given Name and Columns - Example:
                    // create table `product`(`id` int AUTO_INCREMENT primary key, `name` varchar(20), `description` varchar(20), `price` decimal(8,3), `category_id` int);
                    // create table `category` (`id` int AUTO_INCREMENT NOT NULL primary key, `name` varchar(20));

                    String line = reader.readLine();
                    String newTableName = line.substring(line.indexOf("`"), line.indexOf("("));
                    String columns = line.substring(line.indexOf("("), line.lastIndexOf(")") + 2);

                    createTable(newTableName, columns);
                    break;
                case "DROP_TABLE":
                    // 2. Drop Table By Name

                    String dropTable = reader.readLine();

                    dropTable(dropTable);
                    break;
                case "LIST_ALL_TABLES":
                    // 3. List All Tables

                    listAllTables();
                    break;
                case "GET_TABLE_INFO":
                    // 4. Get Table Info

                    String table = reader.readLine();

                    getTableInfo(table);
                    break;
                case "SELECT_PRODUCTS":
                    // 5. Select Products

                    selectProducts();
                    break;
                case "INSERT_DATA_INTO_CATEGORY_TABLE":
                    // 6. Insert Data Into Category Table

                    Integer id = Integer.parseInt(reader.readLine());
                    String name = reader.readLine();

                    insertDataIntoCategoryTable(id, name);
                    break;
                case "INSERT_DATA_INFO_PRODUCT_TABLE":
                    // 6. Insert Data Into Product Table

                    Integer productId = Integer.parseInt(reader.readLine());
                    String productName = reader.readLine();
                    String description = reader.readLine();
                    Double price = Double.parseDouble(reader.readLine());
                    Integer categoryId = Integer.parseInt(reader.readLine());

                    insertDataIntoProductTable(productId, productName, description, price, categoryId);
                    break;
                case "DELETE_PRODUCTS":
                    // 7. Delelte Products in a price range

                    Integer bottom = Integer.parseInt(reader.readLine());
                    Integer top = Integer.parseInt(reader.readLine());

                    deleteProductsWherePriceIsBetween(bottom, top);
                    break;
                case "CREATE_INDEX":
                    // 8. Create Index
                    // Example: CreateIndex SampleId ON product (id)

                    String input = reader.readLine();

                    String indexName = input.substring(input.indexOf(" ") + 1, input.indexOf("ON") - 1);
                    String tableName = input.substring(input.indexOf("ON") + 3, input.indexOf("(") - 1);
                    String columnName = input.substring(input.indexOf("(") + 1, input.indexOf(")"));

                    createIndex(indexName, tableName, columnName);
                    break;
                case "DROP_INDEX":
                    // 9. Drop Index

                    String index = reader.readLine();
                    String tableToDrop = reader.readLine();

                    dropIndex(index, tableToDrop);
                    break;
            }

            System.out.println("INSERT COMMAND, PLEASE: ");
            command = reader.readLine();
        }
    }

    private static void createTable(String tableName, String columns) {
        String query = "create table " + tableName + columns;

        try {
            statement = connection.prepareStatement(query);

            statement.execute();
            statement.close();
            connection.close();

            System.out.println("Table created successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void dropTable(String tableName) {
        String query = "drop table " + tableName;

        try {
            statement = connection.prepareStatement(query);

            statement.execute();
            statement.close();
            connection.close();

            System.out.println("Table dropped successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void listAllTables() {
        try {
            DatabaseMetaData md = connection.getMetaData();
            ResultSet rs = md.getTables(DATABASE_NAME, null, "%", null);

            while (rs.next()) {
                System.out.println(rs.getString(3));
            }

            connection.close();
        } catch (Exception e) {
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

    private static void insertDataIntoCategoryTable(Integer id, String name) {
        QUERY = "INSERT INTO `category`(`id`, `name`) \n" +
                "VALUES (?, ?);";
        try {
            statement = connection.prepareStatement(QUERY);
            statement.setInt(1, id);
            statement.setString(2, name);

            statement.execute();
            statement.close();

            System.out.println("Data into category table has been inserted successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertDataIntoProductTable(Integer id, String name, String description, Double price, Integer categoryId) {
        QUERY = "INSERT INTO `product` (`id`, `name`, `description`, `price`, `category_id`) \n" +
                "VALUES (?, ?, ?, ?, ?);";
        try {
            statement = connection.prepareStatement(QUERY);
            statement.setInt(1, id);
            statement.setString(2, name);
            statement.setString(3, description);
            statement.setDouble(4, price);
            statement.setInt(5, categoryId);

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
