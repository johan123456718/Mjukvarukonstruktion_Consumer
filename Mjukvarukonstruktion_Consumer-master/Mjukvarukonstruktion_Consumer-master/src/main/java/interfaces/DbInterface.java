package interfaces;

import model.JournalRecord;

import java.util.List;

//Interface for database access implementations
public interface DbInterface {

    //Save a journal record to your database
    public void saveToDB(JournalRecord record);
    //Retrieves a journal record based on its id from the database.
    public JournalRecord getJournalRecordById(int id);
    //Retrieves all journal records stored in the database. 
    public List<JournalRecord> getAllJournalRecords();
}

