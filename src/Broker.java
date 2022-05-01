import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/*
 * Broker is the main server, each one of the three stores the information of equal groups/topics.
 */

public class Broker implements Node {
    private List<UserNode> registeredUsers = new ArrayList<>(); //List of registered Users.
    //Από Απορίες 3σελ ( Μια λίστα με registeredUsers και όχι ξεχωριστά για Publishers και Consumers ).
    //private List<Publisher> registeredPublisher = new ArrayList<>();
    private Queue<MultimediaFile> sentFiles = new LinkedList<>(); //All sent texts and files will be stored in a queue.
    private List<Group> existingGroups = new ArrayList<Group>();
    private Info brokerInfo = new Info(); //Contains this broker's information.
    //private ExecutorService pool = Executors.newFixedThreadPool(100); //Broker thread pool.
    private ServerSocket providerSocket; //Broker's server socket, this accepts Consumer queries.
    private int[] ipPort;                //Hashed ip+Port of all the brokers.
    private int brokerId;

    //Broker as a client for publisher
    InputStreamReader inputPublisher = null;
    BufferedReader outPublisher = null;

    public static void main(String[] args) throws IOException {
        Broker b = new Broker();
        int port;
        BufferedReader reader = new BufferedReader(new FileReader("./src/initBroker.txt")); //Reading init file to initialize this broker's port correctly.
        String line;
        line = reader.readLine();
        int brokerNumber = Integer.parseInt(line);
        b.setBrokerId(brokerNumber); //Setting broker's id, based on the initBroker.txt file.
        reader.close();

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("./src/initBroker.txt")));

        if (brokerNumber == 0) { //This means that this is the first broker to be initialized.
            port = FIRSTBROKER;
            brokerNumber++; //Increasing the counter of running brokers.
            System.out.println("Broker Number " + brokerNumber + " with port " + port );
            writer.write(String.valueOf(brokerNumber)); //Refreshing init file.
        } else if (brokerNumber == 1) {
            port = SECONDBROKER;
            brokerNumber++;
            System.out.println("Broker Number " + brokerNumber + " with port " + port );
            writer.write(String.valueOf(brokerNumber));
        } else {
            port = THIRDBROKER;
            System.out.println("Broker Number 3 with port " + port );
            brokerNumber = 0; //When we initialize the third broker we reset the counter to 0.
            writer.write(String.valueOf(brokerNumber));
        }



        //Setting Info ip, port and brokerId.
        b.getBrokerInfo().setIp(ip);
        b.getBrokerInfo().setPort(String.valueOf(port));
        b.getBrokerInfo().setBrokerId(b.getBrokerId());


        writer.close();

        b.calculateKeys(); //Check method.

        b.init(port);
    }

    //Σύνδεση Broker με UserNode αντί Publisher !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    public void init(int port) throws UnknownHostException, IOException{
        providerSocket = new ServerSocket(port);
        System.out.println("[BROKER] "+ getBrokerId() + " Initializing data.");
        int userId = 0;
        Socket initializeSocket = null;
        while(userId<3) {
            if(userId == 0) {
                initializeSocket = new Socket(ip, FIRSTUSER); //This socket will be used to initialize this broker. (Retrieving data from publishers)
            }else if(userId == 1) {
                initializeSocket = new Socket(ip, SECONDUSER);
            }else {
                initializeSocket = new Socket(ip, THIRDUSER);
            }

            PrintWriter initializeQuery = new PrintWriter(initializeSocket.getOutputStream(), true);
            ObjectInputStream initStream = new ObjectInputStream(initializeSocket.getInputStream());

            initializeQuery.println(getIpPort()[getBrokerId()]); //Sending hashed ip+port key to publisher.
            try {
                UserNode u = new UserNode(userId);
                Boolean UserExists = false;
                Group topic = (Group) initStream.readObject(); //Reading the first Group that the UserNode sent.


                while(!topic.getGroupName().equalsIgnoreCase("")) { //Initializing topicList and registeredPublishers. (String "" is a terminal message sent by the publisher).
                    for(UserNode registeredU:registeredUsers) {
                        if(u.getUserID() == registeredU.getUserID()) {
                            UserExists = true;
                        }
                    }
                    if(!UserExists) {
                        getRegisteredUsers().add(u);
                    }
                    System.out.println(topic.getGroupName());
                    u.getGroup().add(topic);

                    getExistingGroups().add(topic);
                    topic = (Group) initStream.readObject(); //Reading the next groupName that the Publisher sent.
                }

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    initStream.close();
                    initializeQuery.close();
                    initializeSocket.close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
            userId++;
            initializeSocket.close();
        }


        //Initializing info list  Info {ListOfBrokers {IP,Port} , < BrokerId, ArtistName>}
        //Στην περίπτωση μας
        ////Initializing info list  Info {ListOfBrokers {IP,Port} , < BrokerId, Group>}
        getBrokerInfo().setExistingGroups(getExistingGroups());
        //Creating file Broker(brokerId).txt which contains the Broker's Info and registeredArtists.
        //Στην περίπτωση μας
        ////Creating file Broker(brokerId).txt which contains the Broker's Info and existingGroups.
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("./src/Broker" + getBrokerId() + ".txt")));
        writer.write(getBrokerInfo().getIp());
        writer.write("\n" + getBrokerInfo().getPort());
        writer.write("\n" + getBrokerInfo().getBrokerId());
        for(Group g:getExistingGroups()) {
            writer.write("\n" + g.getGroupName());
        }
        writer.close();


        while(true) {//Accepting Consumer queries.
            System.out.println("[BROKER] Waiting for consumer connection.");
            Socket client = providerSocket.accept();

            System.out.println("[BROKER] Connected to a consumer!");

            //ActionsForConsumers consumerThread = new ActionsForConsumers(client);
            //pool.execute(consumerThread);

           // Thread consumerThread = new ActionsForConsumers(client);
            //consumerThread.start();
        }

    }


    public int[] getIpPort() {
        return ipPort;
    }

    public void setIpPort(int[] ipPort) {
        this.ipPort = ipPort;
    }


    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public void connect(){};

    public void disconnect(){};

    public void updateNodes(){};

    public List<Broker> getBrokers(){
        return brokers;
    }

    public Info getBrokerInfo() {
        return brokerInfo;
    }

    public void setBrokerInfo(Info brokerInfo) {
        this.brokerInfo = brokerInfo;
    }


    public List<Group> getExistingGroups() {
        return this.existingGroups;
    }

    //public List<Publisher> getRegisteredPublisher() {return registeredPublisher;}

    public List<UserNode> getRegisteredUsers() { return registeredUsers; }


    //δημιουργεί ένα πίνακα hashedKeys[3] που έχει το hashkey κάθε broker
    //το χρησιμοποιεί στον Publisher

    public void calculateKeys() {               //Setting the hashkeys(ip+port) of all the brokers.
        int[] hashedKeys = new int[3];
        String hash;
        MessageDigest md;

        try {
            for (int i = 0; i < 3; i++) {
                if (i == 0) {//Setting the key for the first broker.
                    hash = ip + FIRSTBROKER;
                } else if (i == 1) {
                    hash = ip + SECONDBROKER;
                } else {
                    hash = ip + THIRDBROKER;
                }
                md = MessageDigest.getInstance("MD5");
                md.update(hash.getBytes());
                byte[] digest = md.digest();
                BigInteger no = new BigInteger(1, digest);
                String hashtext = no.toString(16);
                while (hashtext.length() < 32) {
                    hashtext = "0" + hashtext;
                }
                int hashCode = no.hashCode() % 59;
                hashedKeys[i] = hashCode;
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        setIpPort(hashedKeys);
    }




}