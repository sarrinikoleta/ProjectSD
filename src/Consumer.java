import java.io.*;
import java.net.*;
import java.util.*;

import java.security.NoSuchAlgorithmException;
import java.security.MessageDigest;
import java.math.BigInteger;
import java.nio.file.Files; //imports for file searching
import java.nio.file.Paths;
import java.nio.file.Path;

/*
 * Consumer is the user(client) that asks for groups and messages.
 * He receives the chunks from the Broker(server) and creates the chunk files inside the Downloaded Chunks folder.
 */

public class Consumer implements Node {
    private int consumerId;
    private Socket requestSocket = null;
    private PrintWriter initializeQuery;
    private BufferedWriter writer;
    private BufferedReader out;
    private BufferedReader keyboard;
    private InputStreamReader input;
    private ObjectInputStream inB;
    private List<Info> brokerInfo = new ArrayList<>();
    //Scanner scanner = new Scanner(System.in);
    private ProfileName profileName;

    public void init(int port) {
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
                                initializeQuery.println(String.valueOf(getConsumerId())); //sending consumerId to the new broker
                            }
                            initializeQuery.println(topic); //Sending query to the Broker
                        }
                    }
                }


                // ------- FINDING MEDIA FILE --------
                /*
                List<Path> match;
                if (groupFound) {
                    System.out.println("Type the text you want to send. \nIf you want to send media files please type the command: \n 'Send media' and the name of the file you want to send.): ");
                    System.out.println("For example: Send media myphoto.jpg): ");
                    String message = keyboard.readLine();
                    if (message.equalsIgnoreCase("send media")) {
                        String[] filetosend = message.split("send media ");
                        try (Stream<Path> pathStream = Files.find("C", //we want to search in the local drive (c:\\)
                                Integer.MAX_VALUE, //we want to search into all folder levels (subfolder of c) so we set maxDepth=Integer.MAX_VALUE
                                (p, basicFileAttributes) ->
                                        p.getFileName().toString().equalsIgnoreCase(filetosend))
                        ) {
                            match = pathStream.collect(Collectors.toList()); //match is the file found in user's local drive
                        }
                    }
                */

                    //NOT NEEDED
                    //Second selection
                /*

                    Boolean listedSongs = false; // This is set to true after listing the songs of the artist that has been chosen
                    while(true) {
                        String consumerQuery;
                        if(!listedSongs) { //The first query is automatic and will list all of the artists available songs in a string. (The consumer won't receive the actual chunks, just a string with the titles.
                            consumerQuery = "List songs!!!";
                        }else {
                            System.out.println("Type the name of the song you'd like to listen to: (type 'back' to choose a different artist): ");
                            consumerQuery = keyboard.readLine().trim();
                        }


                        if (consumerQuery.equals("back")) {
                            initializeQuery.println(consumerQuery);
                            break;
                        }

                        initializeQuery.println(consumerQuery);

                        Value brokerResponse = (Value) inB.readObject();
                        if(brokerResponse.getMusicFile().getMusicFileExtract() != null){ //Receiving chunks from broker.
                            int c = 0;
                            System.out.println("Downloading song: " + brokerResponse.getMusicFile().getGroupName() + " - " + brokerResponse.getMusicFile().getTrackName() + "...");
                            while(brokerResponse.getMusicFile().getMusicFileExtract() != null) { //Writing chunk mp3 files in the Downloaded Chunks folder.
                                FileOutputStream mp3 = new FileOutputStream("./src/Downloaded Chunks" + "/" + brokerResponse.getMusicFile().getTrackName() + c + ".mp3");
                                mp3.write(brokerResponse.getMusicFile().getMusicFileExtract());
                                mp3.close();
                                c++;
                                brokerResponse = (Value) inB.readObject();
                                if(brokerResponse.getMusicFile().getMusicFileExtract() == null) {
                                    break;
                                }
                            }
                            System.out.println("Your song has been downloaded successfully! Check your Downloaded Chunks folder.");
                        }else { //If the first chunk is null the song doesn't exist.
                            if(!listedSongs) {
                                listedSongs = true;
                            }else {
                                System.out.println("The requested song does not exist.");
                            }
                            System.out.println(topic + "'s available song list.");
                            String availableSongs = out.readLine();
                            for(String songName:availableSongs.split("NEXT")) {//Printing existing songlist of the artist.
                                System.out.println(songName);
                            }
                        }
                    }
                }else {
                    System.out.println("The artist " + topic + " is not available!");
                } */
                }

            } catch(UnknownHostException unknownHost){
                System.err.println("You are trying to connect to an unknown host!");
            } catch(IOException ioException){
                ioException.printStackTrace();
            } catch(ClassNotFoundException e){
                e.printStackTrace();
            }
        }


    // Listening for a message is blocking so need a separate thread for that.
    public void listenForMessage() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                String msgFromGroupChat;
                // While there is still a connection with the server, continue to listen for messages on a separate thread.
                while (requestSocket.isConnected()) {
                    try {
                        // Get the messages sent from other users and print it to the console.
                        msgFromGroupChat = out.readLine();
                        System.out.println(msgFromGroupChat);
                    } catch (IOException e) {
                        // Close everything gracefully.
                        disconnect();
                    }
                }
            }
        }).start();
    }

    public List<Broker> getBrokers(){
        return brokers;
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

    public void disconnect(Broker broker, Group groupName) {} //Disconnects from the group

    public void register(Broker broker, Group groupName) {} //Registers into a group

    //Shows conversation of a specific group (with the name 'groupName')
    public void showConversationData(Group groupName) {}

    @Override
    public void updateNodes() {}

    public Socket getSocket() {
        return requestSocket;
    }

    public BufferedReader getKeyboard(){
        return keyboard;
    }

    public List<Info> getBrokerInfo() {
        return brokerInfo;
    }

    public void setBrokerInfo(List<Info> brokerInfo) {
        this.brokerInfo = brokerInfo;
    }

    public int getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(int consumerId) {
        this.consumerId = consumerId;
    }

    public void setProfileName(ProfileName profileName) {
        this.profileName = profileName;
    }
}

