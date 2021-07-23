import model.JournalRecord;

//Class for parsing messages into objects, or from objects into strings. 
public class Parser {
    
    //Converts a JournalRecord object into a string.
    static String RecordToString(JournalRecord journalRecord){
        StringBuilder builder = new StringBuilder();
        builder.append(journalRecord.getDate()).append("<br>")
                .append(journalRecord.getCategory()).append("<br>")
                .append(journalRecord.getContent());
        return builder.toString();
    }

    //Converts a string into a JournalRecord object.
    static JournalRecord StringToJournalRecord(String string){
        String[] parts = string.split("<br>");
        String date = parts[0];
        String category = parts[1];
        String content = parts[2];
        return new JournalRecord(category, content, date);
    }
}
