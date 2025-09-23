import hdfs.utils.Manager;
import hdfs.operations.Uploader;
import java.util.Scanner;

public class Entry{
    private static final String interactiveModeMenu="""
0. Exit
1. Upload file
""";

    public static void main(String[] args){
        try{
            if(args.length>0)
                handleArgs(args);
            else
                interactiveMode();
            Manager.closeFS();
        }catch(Exception e){
            System.out.println("Error: "+e.getMessage());
            e.printStackTrace();
        }
    }
    private static void handleArgs(String[] args) throws Exception{
        if(args.length<1)
            throw new IllegalArgumentException("Empty arguments");
        String op=args[0];
        switch(op){
            case "upload":
                if(args.length!=4)
                    throw new IllegalArgumentException("Usage: upload <localPath> <hdfsPath> <append|overwrite>");
                boolean append=args[3].equals("append");
                Uploader.upload(args[1],args[2],append);
                break;
            default:
                throw new IllegalArgumentException("Unknown operation: "+op);
        }
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
                        System.out.print("local path: ");
                        String localPath=scanner.next();
                        System.out.print("hdfs path: ");
                        String hdfsPath=scanner.next();
                        System.out.print("append or overwrite ([0]/1): ");
                        boolean append=((scanner.nextInt())!=1);
                        Uploader.upload(localPath,hdfsPath,append);
                        break;
                    default:
                        System.out.println("Unknown choice: "+choice);
                }
            }
            catch(Exception e){
                System.out.println("Error: "+e.getMessage());
                scanner.nextLine();
            }
        scanner.close();
    }
}
