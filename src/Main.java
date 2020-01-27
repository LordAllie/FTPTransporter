import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Pattern;


public class Main {
    static DBConnection connection;
    static PrintStream out;
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static Properties properties = new Properties();
    static String curDir;
    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        Date date = null;
        curDir = System.getProperty("user.dir");
        Connection conn = null;
        try {
            out = new PrintStream(new FileOutputStream("FTPTransporterLogs.txt", true));
            System.setOut(out);
            FileInputStream fileProp = new FileInputStream(curDir + "\\" + "connection.properties");
            properties.load(fileProp);
            connection = new DBConnection();
            conn = connection.openMYConnection();
            final File folder = new File(properties.getProperty("directory"));
            date = new Date();
            System.out.println("Starting Timestamp: " + new Timestamp(date.getTime()));
            connection.deleteRecords();
            processFile(folder);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            date = new Date();
            System.out.println("End Timestamp: " + new Timestamp(date.getTime()));
            connection.closeMYConnection(conn);
        }
    }

    public static void processFile(final File folder) {
        String fileName="";
//        for (final File fileEntry : folder.listFiles()) {
            try {
                Path yourPath = Paths.get(folder.toString()+"\\XenPay.csv");
                File fileEntry = new File(folder.toString()+"\\XenPay.csv");
                Path readPath = Paths.get(curDir + "\\" + "lastDateRead.txt");

                byte[] datebuff = Files.readAllBytes(readPath);
                String dateReadStr = new String(datebuff, Charset.defaultCharset());

//                if(fileEntry.lastModified()!=Long.parseLong(dateReadStr)){
                    fileName = folder.toString() + "\\" + fileEntry.getName();
                    byte[] buff = Files.readAllBytes(yourPath);
                    String s = new String(buff, Charset.defaultCharset());
                    s = s.replaceAll(Pattern.quote("\\~"), "~");
                    String[] strings = s.split("~");
                    String newString="";
                    for (int i = 0; i < strings.length; i++) {
                        if (i != 0) {
                            if (i % 4 == 0) {
                                strings[i] = strings[i].replaceAll(",", "");
                            }
                        }
                    }
                    Files.write(yourPath, String.join("~", strings).getBytes());

                    connection.insertCSV(fileName);
                    String readDate=String.valueOf(fileEntry.lastModified());
                    Files.delete(readPath);
                    Files.createFile(readPath);
                    Files.write(readPath,readDate.getBytes());
                    properties.setProperty("DATE_READ",String.valueOf(fileEntry.lastModified()));
//                }else {
//                    System.out.println("No data to be saved");
//                }
            } catch (Exception e) {
                e.printStackTrace();
            }
//        }

    }
}
