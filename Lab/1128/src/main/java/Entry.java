import java.util.Scanner;
import MapReduce.FileMergeDedup;
import MapReduce.FileSortWithRank;
import MapReduce.FileGrandRelations;
import java.util.Arrays;

public class Entry{
    private static final String interactiveModeMenu="0. Exit\n1. File merge with deduplication\n2. File sort with rank\n3. Grand relations\n";

    private static void handleMD(String[] args) throws Exception{
        FileMergeDedup.main(args);
    }
    private static void handleSWR(String[] args) throws Exception{
        FileSortWithRank.main(args);
    }
    private static void handleGR(String[] args) throws Exception{
        FileGrandRelations.main(args);
    }
    private static void handleArgs(String[] args) throws Exception{
        if(args.length>0){
            switch(args[0]){
                case "MD":
                    if(args.length==4){
                        handleMD(Arrays.copyOfRange(args,1,args.length));
                        break;
                    }
                    throw new IllegalArgumentException("handleArgs(): Usage: MD <inputFile1> <inputFile2> <outputFile>");
                case "SWR":
                    if(args.length>2){
                        handleSWR(Arrays.copyOfRange(args,1,args.length));
                        break;
                    }
                    throw new IllegalArgumentException("handleArgs(): Usage: SWR <inputFile1> [<inputFile2> ...] <outputFile>");
                case "GR":
                    if(args.length==3){
                        handleGR(Arrays.copyOfRange(args,1,args.length));
                        break;
                    }
                    throw new IllegalArgumentException("handleArgs(): Usage: GR <inputFile> <output>");
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
                scanner.nextLine();
                switch(choice){
                    case 0:
                        running=false;
                        System.out.println("Exit");
                        break;
                    case 1:
                        String[] Files1=new String[3];
                        System.out.print("Input file1 path: ");
                        Files1[0]=scanner.nextLine();
                        System.out.print("Input file2 path: ");
                        Files1[1]=scanner.nextLine();
                        System.out.print("Output file path: ");
                        Files1[2]=scanner.nextLine();
                        handleMD(Files1);
                        break;
                    case 2:
                        System.out.print("Number of input files: ");
                        int n=scanner.nextInt();
                        scanner.nextLine();
                        String[] Files2=new String[n+1];
                        for(int i=0;i<n;++i){
                            System.out.print("Input file "+(i+1)+" path: ");
                            Files2[i]=scanner.nextLine();
                        }
                        System.out.print("Output file path: ");
                        Files2[n]=scanner.nextLine();
                        handleSWR(Files2);
                        break;
                    case 3:
                        String[] Files3=new String[2];
                        System.out.print("Input file path: ");
                        Files3[0]=scanner.nextLine();
                        System.out.print("Output file path: ");
                        Files3[1]=scanner.nextLine();
                        handleGR(Files3);
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
