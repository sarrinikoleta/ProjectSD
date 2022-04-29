import java.net.UnknownHostException;
import java.util.*;
import java.io.*;

public class UserNode implements Node{

    private List<Group> groups = new ArrayList<>(); //List of assigned groups that a certain User has.
    private int userID;

    public List<Group> getGroup(){
        return this.groups;
    }


    public void setGroup(List<Group> groups) {
        this.groups = groups;
    }

    public UserNode(int userId) {
        this.userID = userId;
    }

    public int getUserID() {
        return userID;
    }

    public void setUserID(int userID) {
        this.userID = userID;
    }

    public static void main(String[] args) throws IOException{
        Consumer consumer = new Consumer();

        System.out.println("Enter profile name: ");

        ProfileName profileName = new ProfileName(consumer.getKeyboard().readLine());

        consumer.setProfileName(profileName);

        consumer.connect(); //Connecting to a random Broker.

        consumer.init(consumer.getSocket().getPort());

        Publisher publisher = new Publisher(profileName);

        Group group = new Group("Gossip");

        // Infinite loop to read and send messages.
        consumer.listenForMessage();
        publisher.sendMessage();

        System.out.println("Bye!");
    }


    @Override
    public void init(int port) throws UnknownHostException, IOException {

    }

    @Override
    public List<Broker> getBrokers() {
        return null;
    }

    @Override
    public void connect() {

    }

    @Override
    public void disconnect() {

    }

    @Override
    public void updateNodes() {

    }
}
