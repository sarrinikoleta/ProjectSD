import java.io.Serializable;

/*
 * ArtistName objects are used to store an artist's name.
 * Mainly used in the hashing process to find which artist is assigned
 * to a certain Broker.
 */

public class Group implements Serializable{
    private static final long serialVersionUID = 1L;
    private String groupName;

    //Class constructor.

    public Group(String artistName) {
        this.groupName = groupName;
    }

    //Setters and getters of this class.

    public void setGroupName(String groupName){
        this.groupName = groupName;
    }

    public String getGroupName(){
        return this.groupName;
    }
}
