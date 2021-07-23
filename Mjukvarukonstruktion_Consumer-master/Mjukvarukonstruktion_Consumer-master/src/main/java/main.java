import data.MyDatabase;
import java.util.Scanner;

public class main {

    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        System.out.print("Enter role (1=hostpital, 2=Researcher): ");
        int role = scan.nextInt();
        System.out.print("Enter dbType (1=mysql, 2=mongodb): ");
        int dbType = scan.nextInt();
        System.out.print("Enter db port: ");
        int port = scan.nextInt();
        System.out.print("Enter group-ID: ");
        scan.nextLine();
        String groupID = scan.nextLine();
        System.out.println("group id: " + groupID);

        System.out.print("What is the cluster ip address?: ");
        String clusterIP = scan.nextLine();
        System.out.print("What is the cluster port?: ");
        String clusterPort = scan.nextLine();
        MyDatabase.DatabaseConfig(dbType, port, "testuser", "testpassword");

        Consumer consumer = new Consumer(MyDatabase.getinstance(), groupID, clusterIP, clusterPort);
        if(role == 2){
            consumer.consumeMessages("research");
        }else if(role == 1){
            consumer.consumeMessages("replication");
        }
        else throw new IllegalArgumentException("Please select a valid role");

    }

}