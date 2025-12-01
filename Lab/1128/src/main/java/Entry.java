import java.util.Scanner;
import MapReduce.FileMergeDedup;

public class Entry{
    private static final String interactiveModeMenu="0. Exit\n1. File merge with deduplication\n";

    private static void handleMD(String[] args) throws Exception{
        FileMergeDedup.main(new String[]{args[0],args[1],args[2]});
    }
    private static void handleArgs(String[] args) throws Exception{
        if(args.length>0){
            switch(args[0]){
                case "MD":
                    if(args.length!=4)
                        throw new IllegalArgumentException("handleArgs(): Usage: MD <inputFile1> <inputFile2> <outputFile>");
                    handleMD(new String[]{args[1],args[2],args[3]});
                    break;
                default:
                    throw new IllegalArgumentException("handleArgs(): Unknown command: "+args[0]);
            }
            return;
        }
        throw new IllegalArgumentException("handleArgs(): Empty args");
    }
    private static void interactiveMode() throws Exception{
        System.out.print(interactiveModeMenu);
        Scanner scanner=new Scanner(System.in);
        boolean running=true;
        while(running)
            try{
                System.out.print("Enter choice: ");
                int choice=scanner.nextInt();
                switch(choice){
                    case 0:
                        running=false;
                        System.out.println("Exit");
                        break;
                    case 1:
                        System.out.print("Input file1 path: ");
                        String inputFile1=scanner.next();
                        System.out.print("Input file2 path: ");
                        String inputFile2=scanner.next();
                        System.out.print("Output file path: ");
                        String outputFile=scanner.next();
                        handleMD(new String[]{inputFile1,inputFile2,outputFile});
                        break;
                    default:
                        System.err.println("Invalid choice: "+choice);
                }
            }catch(Exception e){
                System.out.println("interactiveMode(): "+e.getMessage());
                scanner.nextLine();
            }
        scanner.close();
    }
    public static void main(String[] args){
        try{
            if(args.length>0)
                handleArgs(args);
            else
                interactiveMode();
        }catch(Exception e){
            System.err.println("main(): "+e.getMessage());
        }
    }
}
