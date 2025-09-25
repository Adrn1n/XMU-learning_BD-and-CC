import hdfs.utils.Controller;
import hdfs.operations.Uploader;
import hdfs.operations.Downloader;
import hdfs.operations.FileReader;
import hdfs.utils.HDFSInfo;
import java.util.List;
import hdfs.operations.Inspector;
import hdfs.operations.Manager;
import java.util.Scanner;

public class Entry{
    private static final String interactiveModeMenu="""
0. Exit
1. Upload (only file can append)
2. Download
3. Read file
4. Inspect info (non-recursive)
5. Inspect info (recursive)
6. Create
7. Delete
""";

    private static void handleUpload(String localPath,String hdfsPath,boolean append) throws Exception{
        Uploader.upload(localPath,hdfsPath,append);
    }
    private static void handleDownload(String hdfsPath,String localPath) throws Exception{
        Downloader.download(hdfsPath,localPath);
    }
    private static void handleRead(String hdfsPath) throws Exception{
        String content=FileReader.read(hdfsPath);
        System.out.print(content);
    }
    private static void printInfo(List<Object> hdfsInfos){
        for(Object obj:hdfsInfos)
            if(obj instanceof HDFSInfo)
                System.out.print(((HDFSInfo)obj).toString());
            else
                printInfo((List<Object>)obj);
    }
    private static void handleInspect(String hdfsPath,boolean recursive) throws Exception{
        List<Object> hdfsInfos=Inspector.getInfo(hdfsPath,recursive);
        printInfo(hdfsInfos);
    }
    private static void handleCreate(String hdfsPath) throws Exception{
        Manager.create(hdfsPath);
    }
    private static void handleDelete(String hdfsPath) throws Exception{
        Manager.delete(hdfsPath);
    }
    private static void handleArgs(String[] args) throws Exception{
        if(args.length<1)
            throw new IllegalArgumentException("Empty arguments");
        String op=args[0];
        switch(op){
            case "upload":
                if(args.length!=4)
                    throw new IllegalArgumentException("Usage: upload <localPath> <hdfsPath> <a (only file)|[o]>");
                handleUpload(args[1],args[2],args[3].equals("a"));
                break;
            case "download":
                if(args.length!=3)
                    throw new IllegalArgumentException("Usage: download <hdfsPath> <localPath>");
                handleDownload(args[1],args[2]);
                break;
            case "read":
                if(args.length!=2)
                    throw new IllegalArgumentException("Usage: read <hdfsPath>");
                handleRead(args[1]);
                break;
            case "inspect":
                if(args.length!=3)
                    throw new IllegalArgumentException("Usage: inspect <hdfsPath> <[r]|nr>");
                handleInspect(args[1],!args[2].equals("nr"));
                break;
            case "create":
                if(args.length!=2)
                    throw new IllegalArgumentException("Usage: create <hdfsPath>");
                handleCreate(args[1]);
                break;
            case "delete":
                if(args.length!=2)
                    throw new IllegalArgumentException("Usage: delete <hdfsPath>");
                handleDelete(args[1]);
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
                            String localPath=scanner.next();
                            System.out.print("hdfs file: ");
                            String hdfsPath=scanner.next();
                            System.out.print("append or overwrite (0 (only file)/[1]): ");
                            boolean append=((scanner.nextInt())==0);
                            handleUpload(localPath,hdfsPath,append);
                            break;
                        }
                    case 2:
                        {
                            System.out.print("hdfs file: ");
                            String hdfsPath=scanner.next();
                            System.out.print("local file: ");
                            String localPath=scanner.next();
                            handleDownload(hdfsPath,localPath);
                            break;
                        }
                    case 3:
                        {
                            System.out.print("hdfs file: ");
                            String hdfsPath=scanner.next();
                            handleRead(hdfsPath);
                            break;
                        }
                    case 4:
                        {
                            System.out.print("hdfs path: ");
                            String hdfsPath=scanner.next();
                            handleInspect(hdfsPath,false);
                            break;
                        }
                    case 5:
                        {
                            System.out.print("hdfs path: ");
                            String hdfsPath=scanner.next();
                            handleInspect(hdfsPath,true);
                            break;
                        }
                    case 6:
                        {
                            System.out.print("hdfs path: ");
                            String hdfsPath=scanner.next();
                            handleCreate(hdfsPath);
                            break;
                        }
                    case 7:
                        {
                            System.out.print("hdfs path: ");
                            String hdfsPath=scanner.next();
                            handleDelete(hdfsPath);
                            break;
                        }
                    default:
                        System.err.println("Unknown choice: "+choice);
                }
            }
            catch(Exception e){
                System.err.println("Error: "+e.getMessage());
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
            Controller.closeFS();
        }catch(Exception e){
            System.err.println("Error: "+e.getMessage());
            e.printStackTrace();
        }
    }
}
