import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;


public class Dstore {

    /* -------------------- command‑line arguments -------------------- */
    private final int port;
    private final int cport;
    private final int timeout;
    private final Path file_folder;

    private Socket controllerSocket;
    private PrintWriter controllerWriter;
    private BufferedReader controllerReader;

    private final Map<String, byte[]> fileMap = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    public static void main(String[] args) throws Exception {
        new Dstore(args).start();
    }

    public Dstore(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: java Dstore <port> <cport> <timeout> <file_folder>");
            System.exit(1);
        }
        port  = Integer.parseInt(args[0]);
        cport = Integer.parseInt(args[1]);
        timeout  = Integer.parseInt(args[2]);
        file_folder = Paths.get(args[3]);
        System.out.println("Listening on port " + port);
    }


    private void start() throws Exception {

        if (!Files.exists(file_folder)) Files.createDirectories(file_folder);
        try (Stream<Path> walk = Files.walk(file_folder)) {
            walk.filter(Files::isRegularFile).forEach(p -> p.toFile().delete());
        }
//JOIN controller
        controllerSocket = new Socket("localhost", cport);
        controllerWriter = new PrintWriter(new OutputStreamWriter(controllerSocket.getOutputStream(), StandardCharsets.UTF_8), true);
        controllerReader = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream(), StandardCharsets.UTF_8));

        controllerWriter.println(Protocol.JOIN_TOKEN + " " + port);
        System.out.println("[Dstore]->[Controller] JOIN sent for port " + port);
//Data socket
        executor.submit(()->{
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                while (true) {
                    Socket peerSocket = serverSocket.accept();
                    executor.submit(() -> handleDataSocket(peerSocket));
                }
            } catch (IOException e) {
                System.out.println("[Dstore] data server error: " + e.getMessage());
            }
        });

//Controller socket
        executor.submit(()->{
            try {
                String line;
                while ((line = controllerReader.readLine()) != null) {
                    handleControllerCommand(line.trim());
                }
            } catch (IOException e) {
                System.out.println("[Dstore] lost connection to Controller: " + e.getMessage());
            }
        });

    }


    private void handleControllerCommand(String line) {
        if (line.isEmpty()) return;
        String[] tokens = line.split(" ");
        switch (tokens[0]) {
            case Protocol.LIST_TOKEN -> handleLIST();
            case Protocol.REMOVE_TOKEN -> {
                if (tokens.length == 2) handleRemove(tokens[1]);
            }
            case Protocol.REBALANCE_TOKEN -> handleREBALANCE(tokens);
            default -> System.out.println("[Dstore] unknown ctrl cmd: " + line);
        }
    }

    private void handleDataSocket(Socket socket) {
        try (socket) {
            socket.setSoTimeout(timeout);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
            InputStream rawIn  = socket.getInputStream();
            OutputStream rawOut = socket.getOutputStream();

            String firstLine = reader.readLine();
            if (firstLine == null) return;
            String[] tokens = firstLine.split(" ");
            switch (tokens[0]) {
                case Protocol.STORE_TOKEN -> handleSTORE(tokens, writer, rawIn);
                case Protocol.LOAD_DATA_TOKEN -> handleLOAD_DATA(tokens, rawOut);
                case Protocol.REBALANCE_STORE_TOKEN -> handleREBALANCING(tokens, writer, rawIn);
                default -> System.out.println(firstLine);
            }
        } catch (IOException e) {
            System.out.println("[Dstore] data socket error: " + e.getMessage());
        }
    }



    private void handleRemove(String fileName) {
        System.out.println("[Controller]->[Dstore] REMOVE " + fileName);
        boolean removed = fileMap.remove(fileName) != null;
        if (removed) {
            try { Files.deleteIfExists(file_folder.resolve(fileName)); } catch (IOException ignored) {}
            controllerWriter.println(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
            System.out.println("[Dstore]->[Controller] REMOVED: " + fileName);
        } else {
            controllerWriter.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
            System.out.println("[Dstore]->[Controller] Couldnt Remove: " + fileName);
        }
    }
    private void handleLIST() {
        System.out.println("[Controller]->[Dstore] LIST");
        StringBuilder reply = new StringBuilder(Protocol.LIST_TOKEN);
        fileMap.keySet().forEach(name -> reply.append(' ').append(name));
        controllerWriter.println(reply);
        System.out.println("[Dstore]->[Controller] LIST reply sent to controller: " + reply);
    }

    private void handleREBALANCE(String[] rawTokens) {
        LinkedList<String> tokens = new LinkedList<>(Arrays.asList(rawTokens));
        tokens.removeFirst();
        if (tokens.isEmpty()) {
            System.out.println("[Dstore] Rebalance: No files to Send/Delete ");
            return;
        }

        int sendCount;

        sendCount = Integer.parseInt(tokens.removeFirst());

         Map<String, List<Integer>> filesToSend = new HashMap<>();
        while (sendCount-- > 0) {
            if (tokens.size() < 2) return;
            String fname = tokens.removeFirst();
            int portNum;
             portNum = Integer.parseInt(tokens.removeFirst());

            if (tokens.size() < portNum) return;

            List<Integer> ports = new ArrayList<>(portNum);
            for (int i = 0; i < portNum; i++) {
                try { ports.add(Integer.parseInt(tokens.removeFirst())); }
                catch (NumberFormatException e) { return; }
            }
            filesToSend.put(fname, ports);
        }

        if (tokens.isEmpty()) {
            System.out.println("[Dstore] Rebalance: No files to Delete ");
            return;        // malformed
        }
        int delCount;
        delCount = Integer.parseInt(tokens.removeFirst());


        List<String> filesToDelete = new ArrayList<>();
        for (int i = 0; i < delCount && !tokens.isEmpty(); i++) {
            filesToDelete.add(tokens.removeFirst());
        }

        boolean ok = handleRebalanceDelete(filesToDelete) & handleRebalanceSend(filesToSend);
        if (ok) controllerWriter.println(Protocol.REBALANCE_COMPLETE_TOKEN);
    }


    private boolean handleRebalanceSend(Map<String, List<Integer>> filesToSend) {
        System.out.println("[Dstore i] -> [Dstore j] Sending files: "+ String.join(" ",filesToSend.keySet()));
        CountDownLatch all = new CountDownLatch(filesToSend.size());

        for (Map.Entry<String, List<Integer>> entry : filesToSend.entrySet()) {
            String filename = entry.getKey();
            byte[] content  = fileMap.get(filename);
            if (content == null) {
                all.countDown();
                continue;
            }

            List<Integer> targetPorts = entry.getValue();
            CountDownLatch perFile = new CountDownLatch(targetPorts.size());

            for (int port : targetPorts) {
                executor.submit(() -> {
                    try (Socket socket = new Socket("localhost", port)) {
                        socket.setSoTimeout(timeout);
                        PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

                        out.println(Protocol.REBALANCE_STORE_TOKEN + " " + filename + " " + content.length);
                        String ackLine = in.readLine();
                        if (!Protocol.ACK_TOKEN.equals(ackLine)) return;

                        OutputStream rawOut = socket.getOutputStream();
                        rawOut.write(content);
                        rawOut.flush();
                    } catch (IOException ignore) {

                    } finally {
                        perFile.countDown();
                    }
                });
            }


            executor.submit(() -> {
                try { perFile.await(timeout, TimeUnit.MILLISECONDS); }
                catch (InterruptedException ignored) {}
                all.countDown();
            });
        }

        try {
            boolean ok = all.await(timeout, TimeUnit.MILLISECONDS);
            System.out.println("Files Sent sucessfully");
            return ok;
        }
        catch (InterruptedException ignored) { return false; }
    }

    private boolean handleRebalanceDelete(List<String> filesToDelete) {
        boolean ok = true;
        for (String fileName : filesToDelete) {
            boolean removedFromMap = fileMap.remove(fileName) != null;
            try {
                Files.deleteIfExists(file_folder.resolve(fileName));
            } catch (IOException e) {
                ok = false;
            }
            if (!removedFromMap) {
                ok = false;
            }
        }
        System.out.println("Succesfully deleted. ");
        return ok;
    }




    private void handleSTORE(String[] tokens, PrintWriter writer, InputStream rawIn) {
        System.out.println("[Client]->[Dstore] Received file from client: " + tokens[1]);
        if (tokens.length != 3) return;
        String fileName = tokens[1];
        int    size;
        try { size = Integer.parseInt(tokens[2]); }
        catch (NumberFormatException e) { return; }

        writer.println(Protocol.ACK_TOKEN);

        try{
            byte[] content = rawIn.readNBytes(size);
            if (content.length != size) return;

            fileMap.put(fileName, content);
            System.out.println("File Stored");
            Files.write(file_folder.resolve(fileName), content);
            System.out.println(Protocol.STORE_ACK_TOKEN);
            controllerWriter.println(Protocol.STORE_ACK_TOKEN + " " + fileName);
        }catch (IOException ignored) {}
    }

    /* -------- LOAD_DATA to client -------- */
    private void handleLOAD_DATA(String[] tokens, OutputStream rawOut) {
        System.out.println("[Client]->[Dstore] Sending file to client: " + tokens[1]);
        if (tokens.length != 2) return;
        String fileName = tokens[1];
        byte[] content = fileMap.get(fileName);
        try {
            if (content == null) {
                rawOut.close();
                return;
            }
            System.out.println("[Client]->[Dstore] File sent to Client: " + tokens[1]);
            rawOut.write(content);
        }catch (IOException ignored){}
    }


    private void handleREBALANCING(String[] tokens, PrintWriter writer, InputStream rawIn) {
        System.out.println("[Dstore i]->[Dstore j] Recieved file :" + tokens[1]);
        if (tokens.length != 3) return;
        String fileName = tokens[1];
        int    size;
        try { size = Integer.parseInt(tokens[2]); }
        catch (NumberFormatException e) { return; }
        writer.println(Protocol.ACK_TOKEN);
        try {
            byte[] content = rawIn.readNBytes(size);
            if (content.length != size) return;

            fileMap.put(fileName, content);
            Files.write(file_folder.resolve(fileName), content);
            System.out.println("[Dstore j] -> [Dstore i] File Stored");
        }catch (IOException ignored){}
    }
}
