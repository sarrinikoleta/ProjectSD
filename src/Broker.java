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
    private List<Consumer> registeredUsers = new ArrayList<>(); //List of registered Consumers.
    private List<Publisher> registeredPublisher = new ArrayList<>();
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
                Publisher p = new Publisher(publisherId);
                Boolean publisherExists = false;
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

            try {
                //I/O streams for the consumer
                in = new InputStreamReader(connection.getInputStream());
                out = new BufferedReader(in);
                printOut = new PrintWriter(connection.getOutputStream(), true);
                outC = new ObjectOutputStream(connection.getOutputStream());

                String consumerQuery = out.readLine();
                if(consumerQuery.equalsIgnoreCase("Initialize broker list.")) { //The Consumer sends this query only the first time they connect to any server.
                    consumerQuery = out.readLine(); //reading consumer id
                    Consumer c = new Consumer();
                    c.setConsumerId(Integer.parseInt(consumerQuery));
                    for(Consumer registeredConsumer:getRegisteredUsers()) {
                        if(!(registeredConsumer.getConsumerId() == c.getConsumerId())) { //adding new customer (if he does not already exist in registeredUser list.
                            getRegisteredUsers().add(c);
                        }
                    }


                    for(int i=0; i<3; i++) { //reading broker info files.
                        BufferedReader readBrokerInfo = new BufferedReader(new FileReader("./src/Broker" + i + ".txt")); //Broker file reader.
                        String line;
                        Info brokerInfo = new Info();

                        line = readBrokerInfo.readLine(); //reading ip
                        brokerInfo.setIp(line);
                        line = readBrokerInfo.readLine(); //reading port
                        brokerInfo.setPort(line);
                        line = readBrokerInfo.readLine(); //reading brokerId
                        brokerInfo.setBrokerId(Integer.parseInt(line));
                        line = readBrokerInfo.readLine();



                        while(line != null) {//reading groupNames
                            brokerInfo.getExistingGroups().add(new Group(line));
                            line = readBrokerInfo.readLine();
                        }
                        readBrokerInfo.close();
                        outC.writeObject(brokerInfo); //sending info to consumer
                        outC.flush();


                    }

                    outC.writeObject(new Info("", "", -1, null));
                }else {//reading consumer id
                    Consumer c = new Consumer();
                    c.setConsumerId(Integer.parseInt(consumerQuery));
                    for(Consumer registeredConsumer:getRegisteredUsers()) {
                        if(!(registeredConsumer.getConsumerId() == c.getConsumerId())) { //adding new customer (if he does not already exist in registeredUser list.
                            getRegisteredUsers().add(c);
                        }
                    }
                }

                /*
                while(true) { //This is where the Broker pulls from the Publisher and pushes to the Consumer the mp3 that's been asked from the query.
                    String artistName = out.readLine(); //Consumer artistName query.

                    for(Publisher p:getRegisteredPublisher()) { //Looking for the groupName in all the registeredPublishers.
                        for(ArtistName artist:p.getArtists()) {
                            if(artist.getArtistName().equalsIgnoreCase(artistName)) {
                                if(p.getPublisherId() == 0) { //Connecting to the correct Publisher.
                                    requestSocket = new Socket(ip, FIRSTPUBLISHER);
                                }else if(p.getPublisherId() == 1) {
                                    requestSocket = new Socket(ip, SECONDPUBLISHER);
                                }else {
                                    requestSocket = new Socket(ip, THIRDPUBLISHER);
                                }
                                //Initializing reader/writer and stream.
                                publisherWriter = new PrintWriter(requestSocket.getOutputStream(), true);
                                inP = new ObjectInputStream(requestSocket.getInputStream());
                                publisherReader = new BufferedReader(new InputStreamReader(requestSocket.getInputStream()));
                            }
                        }
                    }

                    if(requestSocket == null) {
                        break;
                    }
                    publisherWriter.println(artistName);

                    if(artistName.equals("quit")) break; //Terminal message.

                    while(true) { //This is where the Consumer sends song queries.
                        String inputLine = out.readLine(); //This inputLinee is the song query.
                        System.out.println("Client query: " + artistName + " - " + inputLine);

                        if(!inputLine.equalsIgnoreCase("back")) {
                            publisherWriter.println(inputLine);
                            Value publisherResponse = (Value) inP.readObject(); //Pulling first object from publisher.
                            if(publisherResponse.getMusicFile().getMusicFileExtract() != null){ //If the first Value contains a chunk.
                                while(publisherResponse.getMusicFile().getMusicFileExtract() != null) {
                                    outC.writeObject(publisherResponse); //Pushing Value object to the Consumer.
                                    outC.flush();
                                    publisherResponse = (Value) inP.readObject(); //Pulling next Value from Publisher.
                                    if(publisherResponse.getMusicFile().getMusicFileExtract() == null) {
                                        outC.writeObject(publisherResponse);
                                        outC.flush();
                                        break;
                                    }
                                }
                            }else { //If the first chunk is null, that means that the song doesn't exist.
                                outC.writeObject(publisherResponse); //Pushes null chunk to Consumer (to stay in sync).
                                outC.flush();
                                String availableSongs = publisherReader.readLine(); //Pulls from Publisher the available song list for the artist.
                                printOut.println(availableSongs); //Pushes song list metadata to Consumer.
                            }
                        }else { //Breaks this while ("back" is a terminal message for this loop), goes back to ArtistName loop.
                            publisherWriter.println(inputLine); //Sends back command to the Publisher so that they don't get out of sync.
                            break;
                        }
                    }
                }
                */

            } catch (IOException e) {
                e.printStackTrace();
            } //catch (ClassNotFoundException e) {
               // e.printStackTrace();
            //}
            finally {
                try {
                    //Closing sockets, I/O streams, writers/readers.
                    printOut.close();
                    in.close();
                    out.close();
                    connection.close();
                    if(requestSocket != null) {
                        requestSocket.close();
                        publisherReader.close();
                    }
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
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

    public List<Consumer> getRegisteredUsers() { return registeredUsers; }


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