package data;

import interfaces.DbInterface;
import model.JournalRecord;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class mySQL implements DbInterface {

    Connection con;

    public mySQL(String username, String password, int hostPort){
        con = null;
        String server
                = "jdbc:mysql://localhost:"+hostPort+"/patientjournal"
                + "?UseClientEnc=UTF8"
                + "?useTimezone=true&serverTimezone=UTC";

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection(server, username, password);
            System.out.println("Connected!");
        } catch (Exception e){
            System.out.println("Failed to connect to DB");
            e.printStackTrace();
        }
    }

    @Override
    public void saveToDB(JournalRecord record) {
        try {
            String query = "INSERT INTO t_record (category, content, timestamp) values (?,?,?)";
            PreparedStatement statement = con.prepareStatement(query);
            statement.setString(1, record.getCategory());
            statement.setString(2, record.getContent());
            statement.setString(3, record.getDate());
            statement.execute();
        } catch (Exception e){
            System.out.println("Error adding record to DB");
            e.printStackTrace();
        }
    }
    @Override
    public JournalRecord getJournalRecordById(int id) {
        try {
            String query = "SELECT * FROM t_record WHERE id = ?";
            PreparedStatement statement = con.prepareStatement(query);
            statement.setInt(1, id);
            ResultSet rs = statement.executeQuery();
            rs.next();
            JournalRecord record = new JournalRecord(rs.getString("category"), rs.getString("content"), rs.getString("timestamp"));
            return record;
        } catch (Exception e){
            System.out.println("Error getting record from DB");
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<JournalRecord> getAllJournalRecords() {
        try {
            Statement statement = con.createStatement();
            String query = "SELECT * FROM t_record";
            ResultSet rs = statement.executeQuery(query);
            List<JournalRecord> recordList = new ArrayList<>();
            while(rs.next()) {
                JournalRecord record = new JournalRecord(rs.getString("category"), rs.getString("content"), rs.getString("timestamp"));
                recordList.add(record);
            }
            return recordList;
        } catch (Exception e){
            System.out.println("Error getting all records from DB");
            e.printStackTrace();
            return null;
        }
    }

}
