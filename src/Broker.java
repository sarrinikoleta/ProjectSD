import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Broker implements Node {
    private List<Consumer> registeredUsers = new ArrayList<>(); //List of registered Consumers.
    private List<Publisher> registeredPublisher = new ArrayList<>(); //All the publishers that have artists that should be saved in this broker.
    //private List<ArtistName> registeredArtists = new ArrayList<>(); //All the artists that are saved in this broker.


    //!!!!!!!!   Απ ότι κατάλαβα Artists είναι τα δικά μας Groups (topics)
    private List<Group> existingGroups = new ArrayList<Group>();

    private Info brokerInfo = new Info(); //Contains this broker's information.
    //private ExecutorService pool = Executors.newFixedThreadPool(100); //Broker thread pool.

    private ServerSocket providerSocket; //Broker's server socket, this accepts Consumer queries.
    private int[] ipPort;               //Hashed ip+Port of all the brokers.
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

        //b.calculateKeys(); //Check method.

        b.init(port);
    }


    public void init(int port) throws UnknownHostException, IOException{
        providerSocket = new ServerSocket(port);
        System.out.println("[BROKER] "+ getBrokerId() + " Initializing data.");
        int publisherId = 0;
        Socket initializeSocket = null;
        while(publisherId<3) {
            if(publisherId == 0) {
                initializeSocket = new Socket(ip, FIRSTPUBLISHER); //This socket will be used to initialize this broker. (Retrieving data from publishers)
            }else if(publisherId == 1) {
                initializeSocket = new Socket(ip, SECONDPUBLISHER);
            }else {
                initializeSocket = new Socket(ip, THIRDPUBLISHER);
            }

            PrintWriter initializeQuery = new PrintWriter(initializeSocket.getOutputStream(), true);
            ObjectInputStream initStream = new ObjectInputStream(initializeSocket.getInputStream());

            initializeQuery.println(getIpPort()[getBrokerId()]); //Sending hashed ip+port key to publisher.
            try {
                Publisher p = new Publisher();
                Boolean publisherExists = false;
                p.setPublisherId(publisherId);
                Group topic = (Group) initStream.readObject(); //Reading the first Group that the Publisher sent.


                while(!topic.getGroupName().equalsIgnoreCase("")) { //Initializing topicList and registeredPublishers. (String "" is a terminal message sent by the publisher).
                    for(Publisher registeredP:registeredPublisher) {
                        if(p.getPublisherId() == registeredP.getPublisherId()) {
                            publisherExists = true;
                        }
                    }
                    if(!publisherExists) {
                        getRegisteredPublisher().add(p);
                    }
                    System.out.println(topic.getGroupName());
                    p.getGroup().add(topic);
                    getExistingGroups().add(topic);
                    topic = (Group) initStream.readObject(); //Reading the next artistName that the Publisher sent.
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
            publisherId++;
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

            Thread consumerThread = new ActionsForConsumers(client);
            consumerThread.start();
        }

    }


    private class ActionsForConsumers extends Thread {

        private Socket connection;
        private Socket requestSocket = null;
        private PrintWriter printOut;
        private BufferedReader out;
        private InputStreamReader in;
        private BufferedReader publisherReader;
        private ObjectInputStream inP;
        private ObjectOutputStream outC;
        private PrintWriter publisherWriter;
        public ActionsForConsumers(Socket socket) {
            this.connection = socket;
        }
        public void run() {

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

    public List<Publisher> getRegisteredPublisher() {
        return registeredPublisher;
    }



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