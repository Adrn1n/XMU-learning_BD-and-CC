import hdfs.utils.Manager;
import hdfs.operations.Uploader;
import hdfs.operations.Downloader;
import hdfs.operations.FileReader;
import hdfs.operations.Inspector;
import java.util.Map;
import java.util.Scanner;

public class Entry{
    private static final String interactiveModeMenu="""
0. Exit
1. Upload file
2. Download file
3. Read file
4. Inspect info
""";

    private static void handleUpload(String localFile,String hdfsFile,boolean append) throws Exception{
        Uploader.upload(localFile,hdfsFile,append);
    }
    private static void handleDownload(String hdfsFile,String localFile) throws Exception{
        Downloader.download(hdfsFile,localFile);
    }
    private static void handleRead(String hdfsFile) throws Exception{
        String content=FileReader.read(hdfsFile);
        System.out.print(content);
    }
    private static void handleInspect(String hdfsPath) throws Exception{
        Map<String,Object> info=Inspector.getInfo(hdfsPath);
        System.out.println("Permissions: "+info.get("Permissions").toString());
        System.out.println("Size: "+info.get("Size").toString());
        System.out.println("Modification Time: "+info.get("Modification Time").toString());
        System.out.println("Path: "+info.get("Path").toString());
    }
    private static void handleArgs(String[] args) throws Exception{
        if(args.length<1)
            throw new IllegalArgumentException("Empty arguments");
        String op=args[0];
        switch(op){
            case "upload":
                {
                    if(args.length!=4)
                        throw new IllegalArgumentException("Usage: upload <localFile> <hdfsFile> <append|overwrite>");
                    boolean append=args[3].equals("append");
                    handleUpload(args[1],args[2],append);
                    break;
                }
            case "download":
                if(args.length!=3)
                    throw new IllegalArgumentException("Usage: download <hdfsFile> <localFile>");
                handleDownload(args[1],args[2]);
                break;
            case "read":
                if(args.length!=2)
                    throw new IllegalArgumentException("Usage: read <hdfsFile>");
                handleRead(args[1]);
                break;
            case "inspect":
                if(args.length!=2)
                    throw new IllegalArgumentException("Usage: inspect <hdfsPath>");
                handleInspect(args[1]);
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
                        {
                            System.out.print("local file: ");
                            String localFile=scanner.next();
                            System.out.print("hdfs file: ");
                            String hdfsFile=scanner.next();
                            System.out.print("append or overwrite ([0]/1): ");
                            boolean append=((scanner.nextInt())!=1);
                            handleUpload(localFile,hdfsFile,append);
                            break;
                        }
                    case 2:
                        {
                            System.out.print("hdfs file: ");
                            String hdfsFile=scanner.next();
                            System.out.print("local file: ");
                            String localFile=scanner.next();
                            handleDownload(hdfsFile,localFile);
                            break;
                        }
                    case 3:
                        {
                            System.out.print("hdfs file: ");
                            String hdfsFile=scanner.next();
                            handleRead(hdfsFile);
                            break;
                        }
                    case 4:
                        {
                            System.out.print("hdfs path: ");
                            String hdfsPath=scanner.next();
                            handleInspect(hdfsPath);
                            break;
                        }
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
}
