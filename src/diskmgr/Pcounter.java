package diskmgr;

//Creating a pcounter class which works as counter for keeping a track of read and write operations count of pages in different operations
public class Pcounter {
	//variable for read and write count
    public static int rcounter;
    public static int wcounter;

    //function for initialising the variable values 
    public static void initialize() {
        rcounter = 0;
        wcounter = 0;
    }

    //function for incrementing the read counter
    public static void readIncrement() {
        rcounter++;
    }

    //function for incrementing the write counter
    public static void writeIncrement() {
        wcounter++;
    }
}