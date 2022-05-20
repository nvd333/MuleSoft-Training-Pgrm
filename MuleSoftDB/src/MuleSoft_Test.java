import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;



/**
 *
 * @author Nivan Veenith Dsa
 */
public class MuleSoft_Test{
    /**
    * MuleSoft Training program
    */

    public static void createDB(String fileName) {
   
            String url = "jdbc:sqlite:./MuleSoftDB/sql/" + fileName;  
       
            try {  
                Connection conn = DriverManager.getConnection(url);  
                if (conn != null) {  
                    //DatabaseMetaData meta = conn.getMetaData();  
                    //System.out.println("The driver name is " + meta.getDriverName());  
                    System.out.println("A new database has been created.");  
                }  
       
            } catch (SQLException e) {  
                System.out.println(e.getMessage());  
            }  
        } 
    
    public static void makeTable(String dbName,String tableName) {
        String url = "jdbc:sqlite:./MuleSoftDB/sql/" + dbName; 
          
        // SQL statement for creating a new table  
        String sql = "CREATE TABLE IF NOT EXISTS "+tableName+" (\n"  
                + " ID INTEGER PRIMARY KEY,\n"  
                + " NAME VARCHAR2(20),\n"  
                + " ACTOR VARCHAR2(20),\n"  
                + " ACTRESS VARCHAR2(20),\n"
                + " DIRECTOR VARCHAR2(20),\n"
                + " RELEASE_YR NUMBER(4)\n"
                + ");";  
          
        try{  
            Connection conn = DriverManager.getConnection(url); 
            
            Statement stmt = conn.createStatement();  
            stmt.execute(sql);
            System.out.println(tableName+" table created");  
        } catch (SQLException e) {  
            System.out.println(e.getMessage());  
        }  
    }

    public static void insertRecord(String dbName,String tableName,int ID,String Name,String actor,String actress,String director,int year) {
        String sql = "INSERT INTO "+tableName+" VALUES(?,?,?,?,?,?)";  
        String url = "jdbc:sqlite:./MuleSoftDB/sql/" + dbName; 
        try{  
            Connection conn = DriverManager.getConnection(url);  
            PreparedStatement pstmt = conn.prepareStatement(sql);  
            pstmt.setInt(1, ID);  
            pstmt.setString(2, Name);  
            pstmt.setString(3, actor);  
            pstmt.setString(4, actress);  
            pstmt.setString(5, director);  
            pstmt.setInt(6, year);  

            pstmt.executeUpdate();  
            System.out.println("values("+ID+","+Name+","+actor+","+actress+","+director+","+year+") inserted");
        } catch (SQLException e) {  
            System.out.println(e.getMessage());  
        }  
    }  

    private static void rsPrint(String url,String sql){
        try{  
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement(); 
            ResultSet rs    = stmt.executeQuery(sql);  
              
            // loop through the result set  
            while (rs.next()) {  
                System.out.println(rs.getInt("ID") +  "\t" +   
                                   rs.getString("NAME") + "\t" + 
                                   rs.getString("ACTOR") + "\t" +
                                   rs.getString("ACTRESS") + "\t" +
                                   rs.getString("DIRECTOR") + "\t" +
                                   rs.getString("RELEASE_YR"));  
            }  
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } 
    }

    public static void showRecords(String dbName,String tableName) {
        String url = "jdbc:sqlite:./MuleSoftDB/sql/" + dbName; 
        String sql = "SELECT * FROM "+tableName+" ;";
        rsPrint(url, sql);
    }

    public static void showRecords(String dbName,String tableName,String colName,String val) {
        String url = "jdbc:sqlite:./MuleSoftDB/sql/" + dbName; 
        String sql = "SELECT * FROM "+tableName+" WHERE "+colName+"=\""+val+"\";";
        rsPrint(url, sql);
    }
    public static void showRecords(String dbName,String tableName,String colName,int val) {
        String url = "jdbc:sqlite:./MuleSoftDB/sql/" + dbName; 
        String sql = "SELECT * FROM "+tableName+" WHERE "+colName+"="+val+";";
        rsPrint(url, sql);
    }




    
    public static void connect() {
       Connection conn = null;
       try {
           // db parameters
           String url = "jdbc:sqlite:";
           // create a connection to the database
           conn = DriverManager.getConnection(url);
           
           System.out.println("Connection to SQLite has been established.");
           
       } catch (SQLException e) {
           System.out.println(e.getMessage());
       } finally {
           try {
               if (conn != null) {
                   conn.close();
               }
           } catch (SQLException ex) {
               System.out.println(ex.getMessage());
           }
       }
   }

   public static void main(String[] args) {
       createDB("MOV.db");
       makeTable("MOV.db", "movies");


       insertRecord("MOV.DB", "movies", 001, "Bahubali", "Prabhas", "Anushka","SS Rajumouli", 2017);
       insertRecord("MOV.DB", "movies", 002, "RRR", "Ram", "Alia","SS Rajumouli", 2022);
       insertRecord("MOV.DB", "movies", 003, "K.G.F: Chapter 2", "Yash", "Srinidhi","Prashanth Neel", 2022);  
       
       
       showRecords("MOV.db","movies");
       showRecords("MOV.db", "movies","RELEASE_YR",2022);
       
   }
}