package data;

import interfaces.DbInterface;

//Singleton for database access
public class MyDatabase {
    private static DbInterface db;
    
    //Configure which database type you want to use
    public static void DatabaseConfig(int dbType, int hostPort, String username, String password){
        if (dbType == 1)
            db = new mySQL(username, password, hostPort);
        else if (dbType == 2)
            db = new MongoDb(hostPort);
    }

    //returns the singleton
    public static DbInterface getinstance(){
        return db;
    }
}
