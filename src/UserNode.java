import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.io.*;

public class UserNode implements Node{
    private static BufferedReader keyboard;
    private Socket requestSocket;
    private PrintWriter initializeQuery;
    private BufferedWriter writer;
    private BufferedReader out;
    private List<Info> brokerInfo = new ArrayList<>();
    private InputStreamReader input;
    private ObjectInputStream output;
    private ObjectInputStream inB;
    private List<Group> groups = new ArrayList<>(); //List of assigned groups that a certain User has.
    private int userID;
    private ProfileName profileName;

    public UserNode(ProfileName profileName) {
        this.profileName = profileName;
    }


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

    //NEW branch gia test :
    //o userNode kanei syndesi me ton katalilo broker
    //kai meta  ftiaxnoume ena consumer kai enan publisher gia na lamvanoun kai na dinoun messages !


    public static void main(String[] args) throws IOException{

        System.out.println("Enter profile name: ");

        UserNode user = new UserNode(new ProfileName(keyboard.readLine()));

        user.connect();  //Συνδεση του UserNode με εναν τυχαίο Broker

        user.init(getSocket().getPort());

        //dinoume to connection pou exei kanei o userNode ston consumer kai ston publisher
        //wste na lamvanoun kai na stelnoun messages
        Consumer consumer = new Consumer(getSocket());
        Publisher publisher = new Publisher(getSocket());

        //ProfileName profileName = new ProfileName(consumer.getKeyboard().readLine());

        //consumer.setProfileName(profileName);

        //consumer.connect(); //Connecting to a random Broker.

        //consumer.init(consumer.getSocket().getPort());

        //Publisher publisher = new Publisher(profileName);

        Group group = new Group("Gossip");

        // Infinite loop to read and send messages.
       // consumer.listenForMessage();
        //publisher.sendMessage();

        System.out.println("Bye!");
    }


    @Override
    public void init(int port) throws UnknownHostException, IOException {
            try {
                Boolean groupFound;
                initializeQuery.println("Initialize broker list.");
                initializeQuery.println(profileName.getProfileName()); //sending consumerId
                Info info = (Info) inB.readObject();//getting broker's info

                while (!info.getIp().equalsIgnoreCase("")) {
                    getBrokerInfo().add(info);
                    info = (Info) inB.readObject();
                }
                System.out.println("Available group-chats/topics to enter: ");  //printing groups/topics for which a broker is responsible
                for (Info i : getBrokerInfo()) {
                    for (Group topic : i.getExistingGroups()) {
                        System.out.println(topic.getGroupName());
                    }
                }

                while (true) {
                    groupFound = false; //This is set to true if the group/topic from the query exists
                    System.out.println("Type the name of an available group-chat/topic (type 'quit' to disconnect): ");
                    String topic = keyboard.readLine().trim();

                    if (topic.equals("quit")) { //Terminal message
                        initializeQuery.println(topic); //Sends terminal message to Broker so that he can disconnect and terminate the Thread
                        disconnect(); //Disconnecting from the Broker
                        break;
                    }


                    //AYTO THA GINETAI STON Broker-publisher to search ston broker(epistrefei swsto port), to connect ston publisher
                    for (Info i : getBrokerInfo()) { //accessing the info (ip,port,id) of the broker
                        for (Group existingGroups : i.getExistingGroups()) {  //accessing the existing groups/topics of the broker
                            if (topic.equalsIgnoreCase(existingGroups.getGroupName())) { //if the topic the user entered belongs to one of the existing ones
                                groupFound = true; //the group is found
                                if (!(requestSocket.getPort() == Integer.parseInt(i.getPort()))) { //if consumer isn't already connected to the correct broker
                                    initializeQuery.println("quit"); //Sending terminal message to the Broker so that he can disconnect and terminate the Thread
                                    disconnect();
                                    connect(Integer.parseInt(i.getPort())); //connecting to a new broker
                                    //initializeQuery.println(String.valueOf(getConsumerId())); //sending consumerId to the new broker
                                }
                                initializeQuery.println(topic); //Sending query to the Broker
                            }
                        }
                    }
                }

            } catch(UnknownHostException unknownHost){
                System.err.println("You are trying to connect to an unknown host!");
            } catch(IOException ioException){
                ioException.printStackTrace();
            } catch(ClassNotFoundException e){
                e.printStackTrace();
            }
    }


    @Override
    public List<Broker> getBrokers() {
        return null;
    }


    @Override
    public void updateNodes() {

    }
    public void connect() { //Connects to a random Broker and initializes sockets, readers/writers and I/O streams
        try {
            if(requestSocket == null) {
                Random randGen = new Random();
                int random = randGen.nextInt(3);
                if(random == 0) {
                    requestSocket = new Socket(ip, FIRSTBROKER);
                }else if(random == 1) {
                    requestSocket = new Socket(ip, SECONDBROKER);
                }else {
                    requestSocket = new Socket(ip, THIRDBROKER);
                }
            }

            keyboard = new BufferedReader(new InputStreamReader(System.in));
            input = new InputStreamReader(requestSocket.getInputStream());
            out = new BufferedReader(input);
            initializeQuery = new PrintWriter(requestSocket.getOutputStream(), true);
            inB = new ObjectInputStream(requestSocket.getInputStream());

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void connect(int port) { //Connects to a Broker and initializes sockets, readers/writers and I/O streams
        try {
            requestSocket = new Socket(ip, port);
            input = new InputStreamReader(requestSocket.getInputStream());
            out = new BufferedReader(input);
            initializeQuery = new PrintWriter(requestSocket.getOutputStream(), true);
            inB = new ObjectInputStream(requestSocket.getInputStream());

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Disconnects from a Broker and closes sockets, readers/writers and I/O streams/
    public void disconnect() {
        // Note you only need to close the outer wrapper as the underlying streams are closed when you close the wrapper.
        // Note you want to close the outermost wrapper so that everything gets flushed.
        // Note that closing a socket will also close the socket's InputStream and OutputStream.
        // Closing the input stream closes the socket. You need to use shutdownInput() on socket to just close the input stream.
        // Closing the socket will also close the socket's input stream and output stream.
        // Close the socket after closing the streams.
        try {
            if (out != null) {
                out.close();
            }
            if (input != null) {
                input.close();
            }
            if (requestSocket != null) {
                requestSocket.close();
            }
            initializeQuery.close();
            inB.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public List<Info> getBrokerInfo() {
        return brokerInfo;
    }

    public void setBrokerInfo(List<Info> brokerInfo) {
        this.brokerInfo = brokerInfo;
    }


    public void setProfileName(ProfileName profileName) {
        this.profileName = profileName;
    }

    public static Socket getSocket() {
        return requestSocket;
    }
}
