import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;


/**
 * Created by xesi on 24/09/2019.
 */
public class DBConnection {


    private int count;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Calendar currentDate = Calendar.getInstance();
    private final Properties prop;
    private String DATABASE_DRIVER = null;
    private String DATABASE_URL = null;
    private String user = null;
    private String password = null;
    private Connection conn = null;
    private InputStream input;
    private PreparedStatement deleteQuery = null;
    private PreparedStatement getNo = null;
    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public DBConnection() throws SQLException, ClassNotFoundException {
        prop = new Properties();
        try {
            String curDir = System.getProperty("user.dir");
            input = new FileInputStream(curDir + "\\" + "connection.properties");
            prop.load(input);
            this.DATABASE_DRIVER = prop.getProperty("DATABASE_DRIVER");
            this.DATABASE_URL = prop.getProperty("DATABASE_URL");
            this.user = prop.getProperty("user");
            this.password = prop.getProperty("password");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println(">>>>>>>>> Error MYSQL_Connection FileNotFoundException <<<<<<<<<<");
        } catch (IOException ex) {
            ex.printStackTrace();
            System.out.println(">>>>>>>>> Error MYSQL_Connection IOException <<<<<<<<<<");
        }
    }

    public Connection openMYConnection() throws ClassNotFoundException, SQLException {
        try {
            Class.forName(this.DATABASE_DRIVER);
            conn = DriverManager.getConnection(this.DATABASE_URL, this.user, this.password);
            System.out.println("Connection to MYSQL stablished!..");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error in opening Connection to MYSQL!..");
            System.out.println(">>>>>>>>> Error openMYConnection <<<<<<<<<<");
        }
        return conn;
    }

    public void closeMYConnection(Connection connection) {
        String msg = "";
        try {
            if (connection != null)
                connection.close();
            System.out.println("MYSQL connection closed!..");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

    public void deleteRecords() throws SQLException{
        currentDate.setTime(new java.util.Date());
        try {
            String sqldelete = "DELETE FROM payment_reference where billerCode='DEMODU'";
            deleteQuery = conn.prepareStatement(sqldelete);
            deleteQuery.execute();
        } catch (NullPointerException e) {
            e.printStackTrace();
            System.out.println(e.toString());
            System.out.println("Error encountered in deleting table of payment_reference!..");
        } finally {
            System.out.println("BATELEC I payment_reference records deleted successfully.");
        }
    }

    public String insertCSV(String path) {
        currentDate.setTime(new Date());
        String success = "y";
        String newPath=path.replaceAll(Pattern.quote("\\"),"/");
        try {
            String clientCode = "DEMODU";
            String sqlUpdate = "LOAD DATA LOCAL INFILE '"+newPath+"' INTO TABLE payment_reference FIELDS TERMINATED BY '~'  LINES TERMINATED BY '\n' (accountNumber,accountName,billMonth,dueDate,amount) SET billerCode='"+clientCode+"'";
            System.out.println(sqlUpdate);
            Statement stmt = conn.createStatement();
            stmt.execute(sqlUpdate);
        } catch (Exception e) {

            System.out.println(e.toString()+" Error encountered in inserting table of payment_reference!..");
            success = e.getMessage();
        }
        return success;
    }

}
