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
1. Upload
2. Download (auto rename if exists)
3. Read file
4. Inspect info (non-recursive)
5. Inspect info (recursive)
6. Create (dir end with '/')
7. Delete
8. Move
""";

    private static void handleUpload(String localPath,String hdfsPath,Uploader.Mode mode) throws Exception{
        Uploader.upload(localPath,hdfsPath,mode);
    }
    private static void handleDownload(String hdfsPath,String localPath) throws Exception{
        Downloader.download(hdfsPath,localPath,true);
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
        List<Object> hdfsInfos=(Inspector.getInfo(hdfsPath,recursive));
        printInfo(hdfsInfos);
    }
    private static void handleCreate(String hdfsPath) throws Exception{
        new Manager().create(hdfsPath);
    }
    private static void handleDelete(String hdfsPath) throws Exception{
        new Manager().delete(hdfsPath);
    }
    private static void handleMove(String srcPath,String dstPath) throws Exception{
        new Manager().move(srcPath,dstPath);
    }
    private static void handleArgs(String[] args) throws Exception{
        if(args.length<1)
            throw new IllegalArgumentException("Empty arguments");
        String op=args[0];
        switch(op){
            case "upload":
                if(args.length!=4)
                    throw new IllegalArgumentException("Usage: upload <localPath> <hdfsPath> <a|[o]|p>");
                Uploader.Mode mode=null;
                switch(args[3]){
                    case "a":
                        mode=(Uploader.Mode.APPEND);
                        break;
                    case "p":
                        mode=(Uploader.Mode.PREPEND);
                        break;
                    default:
                        mode=(Uploader.Mode.OVERWRITE);
                }
                handleUpload(args[1],args[2],mode);
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
                handleInspect(args[1],!(args[2].equals("nr")));
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
            case "move":
                if(args.length!=3)
                    throw new IllegalArgumentException("Usage: move <srcPath> <dstPath>");
                handleMove(args[1],args[2]);
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
                            System.out.print("append, overwrite or prepend (0/[1]/2): ");
                            Uploader.Mode mode=null;
                            switch(scanner.nextInt()){
                                case 0:
                                    mode=(Uploader.Mode.APPEND);
                                    break;
                                case 2:
                                    mode=(Uploader.Mode.PREPEND);
                                    break;
                                default:
                                    mode=(Uploader.Mode.OVERWRITE);
                            }
                            handleUpload(localPath,hdfsPath,mode);
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
                    case 8:
                        {
                            System.out.print("source hdfs path: ");
                            String srcPath=scanner.next();
                            System.out.print("destination hdfs path: ");
                            String dstPath=scanner.next();
                            handleMove(srcPath,dstPath);
                            break;
                        }
                    default:
                        System.err.println("Unknown choice: "+choice);
                }
            }
            catch(Exception e){
                System.err.println("Error: "+(e.getMessage()));
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
            System.err.println("Error: "+(e.getMessage()));
        }
    }
}
