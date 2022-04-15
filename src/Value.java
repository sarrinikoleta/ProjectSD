import java.io.Serializable;

/* Value objects are used to send the chunks of a certain MultimediaFile from Publisher -> Broker -> Consumer.
 * Serializable makes the objects of this class able to be converted into streams that are going to be
 * sent/received from the ObjectInput/OutputStreams.
 */

public class Value implements Serializable{
    private static final long serialVersionUID = 1L;
    private MultimediaFile multimediaFile;

    //Class constructor.

    public Value(MultimediaFile multimediaFile) {
        this.multimediaFile = multimediaFile;
    }

    //Setters and getters of this class.

    public void setMultimediaFile(MultimediaFile multimediaFile){
        this.multimediaFile = multimediaFile;
    }

    public MultimediaFile getMultimediaFile() {return multimediaFile;}

    //Print returns the MultimediaFile's information (mainly used for debugging).

    public String print() {
        return multimediaFile.getMultimediaFileName() + ": " + multimediaFile.getProfileName() + " " + multimediaFile.getDateCreated() + " "
                + multimediaFile.getLength() + " " + multimediaFile.getFramerate() + " " + multimediaFile.getFrameHeight() + " "
                + multimediaFile.getFrameWidth() + " " + multimediaFile.getMultimediaFileChunk().length;
    }
}
