import java.util.*;
import java.io.*;

public class UserNode {

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




}
