import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.security.MessageDigest;
import java.math.BigInteger;

// ΘΑ ΠΡΕΠΕΙ ΝΑ ΤΑ ΑΝΤΙΚΑΤΑΣΤΗΣΟΥΜΕ ΜΕ ΤΟ PACKAGE https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html

public class Publisher implements Node {
    private ExecutorService pool2 = Executors.newFixedThreadPool(100); //Initializing the thread pool. Each publisher can run 100 threads (queries) in parallel.
    //private ServerSocket providerSocket; //The server socket that accepts the brokers' queries.
    private List<Group> groups = new ArrayList<>(); //List of assigned groups that a certain Publisher has.
    //private List<Mp3File> songs = new ArrayList<>(); //List of all the songs that are assigned to a certain Publisher. (In Mp3File format for easy access to Id3v2/Id3v1 tags).
    //private List<File> filesRead = new ArrayList<>(); //List of all the songs in file format. (Easy access to the byte array of a song).
    private ProfileName profileName;

    private Socket socket = null;   //The publisher has a socket to connect to a broker.
    private BufferedWriter writer;  //And a writer to send messages.


    public Publisher(ProfileName profileName) {
        this.profileName = profileName;
    }

    // Sending a message isn't blocking and can be done without spawning a thread, unlike waiting for a message.
    public void sendMessage() {
        try {
            // Initially send the username of the client.
            writer.write(profileName.getProfileName());
            writer.newLine();
            writer.flush();
            // Create a scanner for user input.
            Scanner scanner = new Scanner(System.in);
            // While there is still a connection with the server, continue to scan the terminal and then send the message.
            while (socket.isConnected()) {
                String messageToSend = scanner.nextLine();
                writer.write(profileName.getProfileName() + ": " + messageToSend);
                writer.newLine();
                writer.flush();
            }
        } catch (IOException e) {
            // Gracefully close everything.
            disconnect();
        }
    }

    /*
    public static void main(String[]args) throws IOException {//,UnsupportedTagException, InvalidDataException{
        Publisher p = new Publisher();
        System.out.println("[PUBLISHER] Reading dataset!");

        //File[] directories = new File("./data").listFiles(File::isDirectory); //Listing all the directories in data folder. (The dataset that contains all the artists and songs).

        /*
         * BufferedReader reads the initPublisher.txt, which contains a number, starting from 0 that
         * indicates how many artists have already been read by the previous Publishers.


        BufferedReader reader = new BufferedReader(new FileReader("./src/initPublisher.txt")); //This reader is going to be used to receive the initPublisher.txt file information.
        String line;
        line = reader.readLine();
        int publisherId =Integer.parseInt(line);
        // int directoriesRead = Integer.parseInt(line); //Storing the number of previously read artists.
        p.setPublisherId(publisherId); //Setting publisher's id.
        int port;
        reader.close();

        //The publisher's port depends on how many artists have already been read by the previous Publishers (if any).

        //Ανάλογα τί αριθμό έχω μέσα στο αρχείο initPublisher
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("./src/initPublisher.txt"))); //This writer is going to be used to update the initPublisher.txt file.
        if(publisherId == 0) { //If no files have been read, that means that this is the first Publisher. We divide by 5 because in our case, each publisher has 5 artists stored.
            port = FIRSTPUBLISHER;
            publisherId++; //Increasing the counter of running publisher
            writer.write(String.valueOf(publisherId)); //Refreshing init file.
        }else if(publisherId == 1) {
            port = SECONDPUBLISHER;
            publisherId++; //Increasing the counter of running publisher
            writer.write(String.valueOf(publisherId)); //Refreshing init file.
        }else {
            port = THIRDPUBLISHER;
            publisherId=0; //we initialize the third publisher we reset the counter to 0.
            writer.write(String.valueOf(publisherId)); //Refreshing init file.
        }

        writer.close();

        //AYTA THA TA KANEI O BROKER STO INIT

        for(int i=directoriesRead; i<directories.length; i++) { //In this for loop, we initialize the publisher's File and Mp3File lists.
            File file = new File(directories[i].getParent() + "/" + directories[i].getName()); //Initializing artist's folder path.
            File[] files = file.listFiles(); //Listing the songs of the artist.
            for(File f: files) { //Storing songs in File and Mp3File lists.
                p.addFile(f);
                p.addSong(new Mp3File(directories[i].getParent() + "/" + directories[i].getName() + "/" + f.getName()));
            }
            p.addArtist(new ArtistName(directories[i].getName())); //Storing the artist's name in ArtistName list.
            directoriesRead++; //After reading all the files for a certain artist we increase this counter.
            if(directoriesRead%5 == 0) break; //After reading 5 artists this for loop breaks.



        if(directoriesRead > directories.length-1) { //This means that all the artists have been stored in a running publisher, so we will reset the counter to 0.
            writer.write(String.valueOf(0)); //Resetting counter.
            writer.close();
        }else { //Else if there are still more artists to be read.
            writer.write(String.valueOf(directoriesRead)); //We update the txt file's counter to the number of currently read artists.
            writer.close();
        }

        if(directoriesRead > directories.length-1) { //This means that all the artists have been stored in a running publisher, so we will reset the counter to 0.
            writer.write(String.valueOf(0)); //Resetting counter.
            writer.close();
        }else { //Else if there are still more artists to be read.
            writer.write(String.valueOf(directoriesRead)); //We update the txt file's counter to the number of currently read artists.
            writer.close();
        }

        System.out.println("Stored artists: ");
        for(ArtistName a: p.getArtists()) {
            System.out.println(a.getArtistName());
        }


         //This section "debugs" the mp3 files.


        for(int i=0; i<p.getSongs().size(); i++) {// Debugging mp3 files creating ID3v1 tags for every file that doesn't have one.

            Mp3File song = p.getSongs().get(i);
            ID3v1 id3v1Tag;
            if(!song.hasId3v1Tag()) {
                id3v1Tag = new ID3v1Tag();
                song.setId3v1Tag(id3v1Tag);
                //Setting song information on id3v1tag

                id3v1Tag.setTrack(song.getId3v2Tag().getTrack());
                id3v1Tag.setArtist(song.getId3v2Tag().getArtist());
                id3v1Tag.setTitle(p.getFilesRead().get(i).getName().split(".mp3")[0]);
                id3v1Tag.setAlbum(song.getId3v2Tag().getAlbum());
                id3v1Tag.setGenre(song.getId3v2Tag().getGenre());
            }else {
                if(song.hasId3v2Tag()) {
                    if(song.getId3v1Tag().getTrack() == null) song.getId3v1Tag().setTrack(song.getId3v2Tag().getTrack());
                    if(song.getId3v1Tag().getArtist() == null) song.getId3v1Tag().setArtist(song.getId3v2Tag().getArtist());
                    song.getId3v1Tag().setTitle(p.getFilesRead().get(i).getName().split(".mp3")[0]);
                    if(song.getId3v1Tag().getAlbum() == null) song.getId3v1Tag().setAlbum(song.getId3v2Tag().getAlbum());
                    if(song.getId3v1Tag().getGenreDescription() == null) song.getId3v1Tag().setGenre(song.getId3v2Tag().getGenre());
                }
            }
        }


        p.init(port); //Finally we initialize the Publisher.
    }
    */

    public void getBrokerList() {}


    // Method hashTopic returns the topicList for a certain hashKey.
    //calculate MD5 hash value

    public List<Group> hashTopic(List<Group> groupList, int hashKey){
        List<Group> topic = new ArrayList<>();
        int[] ipPort = new int[3];
        String hash;
        MessageDigest md;
        try {
            for(int i=0; i<3; i++) { //Creating hashKeys for all the brokers' ip+port.
                if(i==0) {//Setting the key for the first broker.
                    hash = ip + FIRSTBROKER;
                }else if(i==1) {
                    hash = ip + SECONDBROKER;
                }else {
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
                int hashCode = no.hashCode()%59;
                ipPort[i] = hashCode;
            }
            Arrays.sort(ipPort); //sorting array to find where the hashkey belongs easier.

            for(Group group:groupList) {//Creating hashKeys for all the groups.
                Boolean f = false; //this will remain false if the groupName(key) > ipPort[](keys)
                String groupName = group.getGroupName();
                md = MessageDigest.getInstance("MD5");
                md.update(groupName.getBytes());
                byte[] digest = md.digest();
                BigInteger no = new BigInteger(1, digest);
                String hashtext = no.toString(16);
                while (hashtext.length() < 32) {
                    hashtext = "0" + hashtext;
                }
                int hashCode = no.hashCode()%59;

                for(int hashedIpPort:ipPort) {
                    if(hashCode <= hashedIpPort){
                        if(hashedIpPort == hashKey) {
                            topic.add(new Group(groupName));
                            f = true;
                            break;
                        }else {
                            break;
                        }
                    }
                }
                if(!f) { //If the hashkey of the group > highest broker haskey, then assign this group to the broker if he has the smallest.
                    if(hashKey == ipPort[0] && hashCode > ipPort[ipPort.length-1]) {
                        topic.add(new Group(groupName));
                    }
                }

            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return topic;
    }


    @Override
    public void init(int port) throws UnknownHostException, IOException{
        providerSocket = new ServerSocket(port);
        int brokerCount = 0; //This will be used to make sure all brokers are initialized.


        System.out.println("[PUBLISHER] Initializing Brokers!");
        while(brokerCount<3) { //When initialized, the Publisher waits for all 3 Brokers to be connected to them.
            Socket brokerInit = providerSocket.accept();
            System.out.println("Initializing Broker " + brokerCount + "...");
            //Initializing Streams to communicate the data between Publisher and Broker.
            InputStreamReader inS = new InputStreamReader(brokerInit.getInputStream());
            BufferedReader brokerReader = new BufferedReader(inS);
            ObjectOutputStream out = new ObjectOutputStream(brokerInit.getOutputStream());

            String brokerQuery = brokerReader.readLine(); //Broker query.

            int hashkey = Integer.parseInt(brokerQuery); //Receiving the broker's ip+port hashkey.
            List<Group> topicList = hashTopic(getGroup(), hashkey); //Creates the topicList for the broker, which contains all the Groups that have to be assigned to the Broker based on his hashkey.
            for(Group group:topicList) {
                out.writeObject(group); //Pushing topicList to the broker.
                out.flush();
            }
            out.writeObject(new Group("")); //Sending terminal message.
            out.flush();

            //Finally the publisher closes the streams that are connected to the broker and increases the counter.
            out.close();
            brokerInit.close();
            brokerCount++;
        }

        while(true) { //After initializing all the brokers, Publisher is ready to accept queries.
            System.out.println("[PUBLISHER] Waiting for broker connection.");
            Socket client = providerSocket.accept();
            System.out.println("[PUBLISHER] Connected to a broker!");
           // ActionsForBrokers brokerThread = new ActionsForBrokers(client); //Each query creates a new Thread.
           // pool2.execute(brokerThread); //Threadpool executes brokerThread.
        }
    }


    //ActionsForBrokers

    /*
    private class ActionsForBrokers extends Thread {
        private Socket requestSocket;   //Broker's socket.
        //Reader/writers and I/O streams.
        //private PrintWriter avalableSongStream;
        private BufferedReader outS;
        private InputStreamReader inS;
        private ObjectOutputStream out;

        public ActionsForBrokers(Socket socket) {
            this.requestSocket = socket;
        }

        public void push(String requestedArtist, String requestedSong) { //Pushes value objects.
            Boolean songFound = false; //This is used to indicate whether or not the song has be en found in the dataset.
            System.out.println("Broker query: " + requestedArtist + " " + requestedSong);
            Value v = null;
            MusicFile m = null;
            Mp3File s = null;
            try {
                for (int i = 0; i < songs.size(); i++) { //Checking if the requested song exists in the songs list.
                    if (requestedSong.equalsIgnoreCase(songs.get(i).getId3v1Tag().getTitle()) && requestedArtist.equalsIgnoreCase(songs.get(i).getId3v1Tag().getArtist())) {
                        songFound = true;
                        s = songs.get(i);
                        FileInputStream is = new FileInputStream(filesRead.get(i)); //Reading the mp3 file matching the broker query.
                        byte[] chunk = new byte[512 * 1024]; //Creating the chunk array and setting how many bytes each chunk is.

                        int rc = is.read(chunk); //Reading the first chunk of the file.
                        while (rc != -1) { //This keeps reading and splitting the mp3 file until its completely read.

                            //Storing the information of the song in MusicFile and Value objects.
                            m = new MusicFile(s.getId3v1Tag().getTitle(), s.getId3v1Tag().getArtist(), s.getId3v1Tag().getAlbum(), s.getId3v1Tag().getGenreDescription(), chunk);
                            v = new Value(m);
                            out.writeObject(v); //Sending the Value object through the ObjectOutputStream.
                            out.flush();
                            chunk = new byte[512 * 1024];
                            rc = is.read(chunk); //Reading next chunk.
                        }
                        is.close(); //Closing FileInputStream.
                    }
                }
                m = new MusicFile("", "", "", "", null); //Creates terminal musicFile.
                v = new Value(m);
                out.writeObject(v); //Sends terminal value.
                out.flush();
                if (!songFound) {
                    notifyFailure(requestedArtist); //Sending the list of all the available songs the requested artist has to the broker.
                }
            } catch (IOException e) {
                e.printStackTrace();
            }


        }

        public void notifyFailure(String requestedArtist) { //If the requested song does not exist, the Publisher notifies the Broker by sending the current available song list.
            /*
            String listedSongs = "";
            for(File f: filesRead) {
                if(f.getParentFile().getName().equalsIgnoreCase(requestedArtist)) { //Creating the listedSongs String.
                    listedSongs = listedSongs + f.getName().split(".mp3")[0] + "NEXT"; //"NEXT" is used by the Consumer to list the songs correctly on the screen.
                }
            }
            avalableSongStream.println(listedSongs); //sending songList.


        }

        public void run() {
            try {
                //Initializing streams.
                avalableSongStream = new PrintWriter(requestSocket.getOutputStream(), true);
                inS = new InputStreamReader(requestSocket.getInputStream());
                outS = new BufferedReader(inS);
                out = new ObjectOutputStream(requestSocket.getOutputStream());

                while (true) { //This while breaks when the broker sends the command quit. (The thread then terminates).
                    //Broker query
                    String requestedArtist = outS.readLine(); //The requested artist's name is stored in this string.
                    if (requestedArtist.equalsIgnoreCase("quit")) break;

                    while (true) { //This while breaks when the broker sends the command back.
                        String requestedSong = outS.readLine(); //The requested songs's name is stored in this string.
                        if (!requestedSong.equalsIgnoreCase("back")) {
                            push(requestedArtist, requestedSong); //Pushes the information to the broker. (Read method push for more info).
                        } else {
                            break;
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {//Closing streams and sockets.
                    avalableSongStream.close();
                    out.close();
                    inS.close();
                    outS.close();
                    requestSocket.close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        }

    }


    */

    @Override
    public void connect() {}
    @Override
    public void disconnect() {

    }
    @Override
    public void updateNodes() {}

      /*
    public void addFile(File f) {
        this.filesRead.add(f);
    }

    public void addSong(Mp3File song) {
        this.songs.add(song);
    }

    public void addArtist(ArtistName artistName) {
        this.groups.add(artistName);
    }

    public List<Mp3File> getSongs(){
        return this.songs;
    }

     public List<File> getFilesRead(){
        return this.filesRead;
    }

*/
    public List<Group> getGroup(){
        return this.groups;
    }


    public void setGroup(List<Group> groups) {
        this.groups = groups;
    }

    @Override
    public List<Broker> getBrokers() {
        return brokers;
    }




}
