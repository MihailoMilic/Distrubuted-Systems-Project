import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


public class Controller {

    private final int cport;
    private final int R;
    private final int timeout;//milisec
    private final int rebalancePeriod; //sec


    private final ServerSocket serverSocket;
    private final ExecutorService executor = Executors.newCachedThreadPool();



    private static class DataStore {
        final int dport;
        final Socket controlSocket;
        final BufferedReader reader;
        final PrintWriter writer;
        final Set<String> fileSet = ConcurrentHashMap.newKeySet();

        DataStore(int port, Socket socket, BufferedReader in, PrintWriter out) {
            dport = port;
            controlSocket = socket;
            reader = in;
            writer = out;
        }
    }

    private final Map<Integer, DataStore> dataStores = new ConcurrentHashMap<>();

    private final String STORING = "storing";
    private final String STORED = "stored";
    private final String REMOVING = "removing";


    private static class FileInfo {
        final String fileName;
        final int fileSize;
        volatile String state;
        volatile CountDownLatch latch;

        FileInfo(String name, int size, String state) {
            fileName = name;
            fileSize = size;
            this.state = state;
        }
    }

    private final Map<String, FileInfo> fileIndex = new ConcurrentHashMap<>();


    private final ReentrantLock rebalanceMutex = new ReentrantLock();
    private volatile CountDownLatch listLatch;
    private volatile CountDownLatch rebalancePhaseLatch;


    public static void main(String[] args){
        try {
            new Controller(args).start();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private Controller(String[] args) throws IOException  {
        if (args.length != 4) {
            System.err.println("Usage: java Controller cport R timeout rebalance_period");
            System.exit(1);
        }
        cport = Integer.parseInt(args[0]);
        R = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        rebalancePeriod = Integer.parseInt(args[3]);

        serverSocket = new ServerSocket(cport);

        System.out.println("[Controller] listening on " + cport);


        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::rebalanceSafely, rebalancePeriod, rebalancePeriod, TimeUnit.SECONDS);
    }

    private final BlockingQueue<Socket> pendingSockets = new LinkedBlockingQueue<>();

    private void start(){
//        handle connections
        executor.submit(()->{
            while (true) {
                try {
                    Socket socket = pendingSockets.take();
                    rebalanceMutex.lock();
                    try {
                        executor.submit(() -> handleConnection(socket));
                    } finally {
                        rebalanceMutex.unlock();
                    }
                } catch (InterruptedException e) {
                    System.out.println("[Controller] Error during handling of socket connection");
                }
            }
        });
//        handle conncurrent calls
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                pendingSockets.add(socket);
            }catch (IOException e) {
                System.out.println("[Controller] Error during handling of socket connection");
            }

        }
    }

    private void handleConnection(Socket socket) {

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
            String firstLine;
            final Set<Integer> triedPorts = new HashSet<>();
            boolean prevMethodLoad;
            while ((firstLine = reader.readLine()) != null) {
                String[] tokens = firstLine.split(" ");
                switch (tokens[0]) {
                    case Protocol.JOIN_TOKEN -> {

                        rebalanceMutex.lock();
                        try {
                            handleJOIN(tokens, socket, reader, writer);
                        } finally {
                            rebalanceMutex.unlock();
                        }
                        return;
                    }
                    case Protocol.STORE_TOKEN -> {
                        handleSTORE(tokens, writer);
                        triedPorts.clear();
                    }
                    case Protocol.LOAD_TOKEN-> {
                        triedPorts.clear();
                        handleLOAD(tokens, writer, triedPorts);
                    }

                    case Protocol.RELOAD_TOKEN -> handleLOAD(tokens, writer, triedPorts);
                    case Protocol.REMOVE_TOKEN -> {
                        handleREMOVE(tokens, writer);
                        triedPorts.clear();
                    }
                    case Protocol.LIST_TOKEN -> {
                        handleLIST(writer);
                        triedPorts.clear();
                    }
                    default -> {
                        triedPorts.clear();
                        System.out.println("[Controller ] " + firstLine);
                    }
                }
            }
        } catch (Exception ex) {
            System.err.println("[Controller] connection error: " + ex);
        } finally {
            rebalanceMutex.unlock();
        }
    }

    private void handleJOIN(String[] joinTokens, Socket socket, BufferedReader reader, PrintWriter writer) {
        int port = Integer.parseInt(joinTokens[1]);
        if (dataStores.containsKey(port)) {
            try {
                socket.close();
            } catch (IOException e) {
                System.err.println("[Controller] Error closing socket: " + e);
            }
            return;
        }
        DataStore store = new DataStore(port, socket, reader, writer);
        System.out.println("[Controller] joining data store" + port);
        dataStores.put(port, store);
        executor.submit(() -> {
            try {
                String line;
                while ((line = store.reader.readLine()) != null) {
                    String[] parts = line.split(" ");
                    switch (parts[0]) {
                        case Protocol.STORE_ACK_TOKEN -> onStoreAck(parts[1], store);
                        case Protocol.REMOVE_ACK_TOKEN -> onRemoveAck(store, parts[1]);
                        case Protocol.LIST_TOKEN -> onListResponse(store, parts);
                        case Protocol.REBALANCE_COMPLETE_TOKEN -> onRebalanceComplete();
                        default -> System.out.println(line);
                    }
                }
            } catch (IOException ignored) {
            } finally {
                dataStores.remove(store.dport);
                try {
                    store.controlSocket.close();
                } catch (IOException ignored) {
                }
            }
        });
        rebalanceSafely();
    }
    private void onRebalanceComplete(){
        if (rebalancePhaseLatch != null) {
            rebalancePhaseLatch.countDown();
        }
    }
    private void handleSTORE(String[] tokens, PrintWriter clientWriter) {
        System.out.println("[Controller] Recieved STORE from client: " + tokens[0] + ' ' + tokens[1]);
        if (dataStores.size() < R) {
            clientWriter.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }
        String fileName = tokens[1];
        int fileSize = Integer.parseInt(tokens[2]);
        FileInfo fileinfo = new FileInfo(fileName, fileSize, STORING);
        fileinfo.latch = new CountDownLatch(R);
        synchronized (fileIndex) {
            if (fileIndex.containsKey(fileName)) {
                clientWriter.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                return;
            }
            fileIndex.put(fileName, fileinfo);
        }


        StringBuilder reply = new StringBuilder(Protocol.STORE_TO_TOKEN);
        for (DataStore dstore : dataStores.values()) {
            reply.append(' ').append(dstore.dport);
        }
        System.out.println("[Controller]->[Client] " + reply);
        clientWriter.println(reply);
        executor.submit(() -> awaitStoreAcks(fileName, clientWriter));
    }

    private void awaitStoreAcks(String fileName, PrintWriter clientWriter) {
        System.out.println("Waiting for STORE ACKs");
        FileInfo record = fileIndex.get(fileName);
        try {
            boolean storedSuccesfully = record.latch.await(timeout, TimeUnit.MILLISECONDS);
            if (storedSuccesfully) {
                record.state = STORED;
                clientWriter.println(Protocol.STORE_COMPLETE_TOKEN);
            } else {
                fileIndex.remove(fileName);
            }
        } catch (InterruptedException e) {
            System.out.println("Something with awaitStoreAcks - COntroller");
        }
    }

    private void onStoreAck(String fileName, DataStore store) {
        FileInfo fileinfo = fileIndex.get(fileName);
        store.fileSet.add(fileName);
        if (fileinfo != null && Objects.equals(fileinfo.state, STORING)) {
            System.out.println("[Dstore] -> [Controller] file acknowledged: " + fileName);
            fileinfo.latch.countDown();
        }
    }

    private void handleLOAD(String[] tokens, PrintWriter clientWriter, Set<Integer> triedPorts ) {
        System.out.println("[Client] -> [Controller] " + Arrays.toString(tokens));

        if (dataStores.size() < R) {
            System.out.println("[Controller]->[Client] " + Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            clientWriter.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }

        String fileName = tokens[1];
        FileInfo fileinfo;
        synchronized (fileIndex) {
            fileinfo = fileIndex.get(fileName);
            if (fileinfo == null || !Objects.equals(fileinfo.state, STORED)) {
                clientWriter.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }
        }
        List<DataStore> dstores = dataStores.values().stream().filter(d -> d.fileSet.contains(fileName)).toList();

        triedPorts.retainAll(dstores.stream().map(ds -> ds.dport).collect(Collectors.toSet()));

        if (triedPorts.size() == dstores.size()){
            clientWriter.println(Protocol.ERROR_LOAD_TOKEN);
            return;
        }
        for (DataStore ds : dstores) {
            if (!triedPorts.contains(ds.dport)) {
                triedPorts.add(ds.dport);
                clientWriter.println(Protocol.LOAD_FROM_TOKEN + " " + ds.dport + " " + fileinfo.fileSize);
                return;
            }
        }

        clientWriter.println(Protocol.ERROR_LOAD_TOKEN);
    }

    private void handleREMOVE(String[] tokens, PrintWriter clientWriter) {
        System.out.println("[Client] -> [Controller]" + Arrays.toString(tokens));
        if (dataStores.size() < R) {
            clientWriter.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }
        String fileName = tokens[1];
        FileInfo fileinfo = fileIndex.get(fileName);
        synchronized (fileIndex) {
            if (fileinfo == null || !Objects.equals(fileinfo.state, STORED)) {
                clientWriter.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }
            fileinfo.state = REMOVING;
        }
        fileinfo.latch = new CountDownLatch(R);
        for (DataStore ds : dataStores.values()) {
            if (ds.fileSet.contains(fileName)) {
                ds.writer.println(Protocol.REMOVE_TOKEN + ' ' + fileName);
            }
        }
        executor.submit(() -> awaitRemoveAcks(fileName, clientWriter));
    }

    private void awaitRemoveAcks(String fileName, PrintWriter clientWriter) {
        System.out.println("Awaiting Remove ACKs");
        FileInfo rec = fileIndex.get(fileName);
        try {
            if (rec.latch.await(timeout, TimeUnit.MILLISECONDS)) {
                fileIndex.remove(fileName);
                clientWriter.println(Protocol.REMOVE_COMPLETE_TOKEN);
            }
        } catch (InterruptedException ignored) {
        }
    }

    private void onRemoveAck(DataStore dstore, String fileName) {
        FileInfo fileinfo = fileIndex.get(fileName);
        synchronized (fileIndex) {
            if (fileinfo != null && Objects.equals(fileinfo.state, REMOVING)) {
                dstore.fileSet.remove(fileName);
                fileinfo.latch.countDown();
            }
        }
    }

    private void handleLIST(PrintWriter clientWriter) {
        System.out.println("[Client] -> [Controller] LIST");
        StringBuilder message = new StringBuilder(Protocol.LIST_TOKEN);

        fileIndex.forEach((name, rec) -> {
            if (Objects.equals(rec.state, STORED)) {
                message.append(' ').append(name);
            }
        });
        System.out.println("[Controller]->[Client] " + message);
        clientWriter.println(message);
    }

    private boolean hasPendingOperation() {
        boolean pending;
        synchronized (fileIndex) {
            pending = fileIndex.values().stream().anyMatch(r -> !Objects.equals(r.state, STORED));
        }
        System.out.println(" PENDING OPERATIONS:  " + pending);
        return pending;
    }

    private void rebalanceSafely() {
        if (!rebalanceMutex.tryLock()) {
            return;
        }
        try {
            while (hasPendingOperation()) {
                System.out.println("Wait for Operations to finish");
            }
            if (dataStores.size() >= R) {
                performRebalance();
            } else {
                System.out.println("Not enough dstores joined for rebalance");
            }
        } finally {
            rebalanceMutex.unlock();
        }
    }

    private void onListResponse(DataStore store, String[] parts) {
        System.out.println("[Controller] Dstore " + store.controlSocket + " listed their files.");
        store.fileSet.clear();

        if (parts.length > 1) {
            store.fileSet.addAll(Arrays.asList(parts).subList(1, parts.length));
        }
        ;
        if (listLatch != null) {
            listLatch.countDown();
        }
    }

    private void performRebalance() {
        System.out.println("PERFORMING REBALANCE");

//       step 1 - ask for lists

        listLatch = new CountDownLatch(dataStores.size());
        for (DataStore dstore : dataStores.values()) {
            dstore.writer.println(Protocol.LIST_TOKEN);
        }
        try {
            listLatch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
        }

//step 2 - make a plan
        System.out.println("Step 2. Build plan");
        Map<DataStore, RebalancePlan> plans = buildRebalancePlans();
        /* step3: execute plans */
        System.out.println("Step 3: execute plan");
        rebalancePhaseLatch = new CountDownLatch(dataStores.size());
        plans.forEach(this::sendPlanToStore);
        try {
            rebalancePhaseLatch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    private static class RebalancePlan {
        final Map<String, List<Integer>> filesToSend = new HashMap<>(); // file -> list of ports
        final Set<String> filesToDelete = new HashSet<>();
    }

    private Map<DataStore, RebalancePlan> buildRebalancePlans() {
        System.out.println("Building Rebalance Plans");
        int N = dataStores.size();
        long F = fileIndex.values().stream().filter(rec -> Objects.equals(rec.state, STORED)).count();
        int minPerStore = (int) Math.floor((double) (R * F) / N);
        int maxPerStore = (int) Math.ceil((double) (R * F) / N);

        Map<DataStore, RebalancePlan> planMap = new HashMap<>();
        for (DataStore dstore : dataStores.values()) {
            planMap.put(dstore, new RebalancePlan());
        }
//         map file -> current holders
        Map<String, Set<DataStore>> fileHolders = new HashMap<>();
        for (DataStore dstore : dataStores.values()) {
            for (String fileName : dstore.fileSet) {
                if (!fileHolders.containsKey(fileName)) {
                    fileHolders.put(fileName, new HashSet<>());
                } else {
                    fileHolders.get(fileName).add(dstore);
                }

            }
        }


//       Make sure each file has R dstores
        for (Map.Entry<String, Set<DataStore>> entry : fileHolders.entrySet()) {

            String file = entry.getKey();
            Set<DataStore> dstores = entry.getValue();
            FileInfo fileInfo = fileIndex.get(file);

            synchronized (fileIndex) {
                if (fileInfo == null || !Objects.equals(fileInfo.state, STORED)) {
                    // file not supposed to exist -> delete everywhere
                    for (DataStore dstore : dstores) {
                        planMap.get(dstore).filesToDelete.add(file);
                    }
                    continue;
                }
            }
            if (dstores.size() < R) { // need extra replicas
                int needed = R - dstores.size();

                List<DataStore> receivers = dataStores.values().stream().filter(dstore -> !dstores.contains(dstore)).sorted(Comparator.comparingInt(ds -> ds.fileSet.size())).limit(needed).toList();
                DataStore sender = dstores.iterator().next();

                planMap.get(sender).filesToSend.put(file, receivers.stream().map(r -> r.dport).toList());
                for (DataStore dstore : receivers) {
                    dstores.add(dstore);
                    dstore.fileSet.add(file);
                }
            } else if (dstores.size() > R) { 
                int excess = dstores.size() - R;
                List<DataStore> toBeRemoved = dstores.stream().sorted(Comparator.comparingInt(dstore -> dstore.fileSet.size())).limit(excess).toList().reversed();
                for (DataStore dstore : toBeRemoved) {
                    planMap.get(dstore).filesToDelete.add(file);
                    dstores.remove(dstore);
                    dstore.fileSet.remove(file);
                }
            }
        }
        return planMap;
    }

    private void sendPlanToStore(DataStore store, RebalancePlan plan) {

        StringBuilder message = new StringBuilder(Protocol.REBALANCE_TOKEN).append(' ');

        message.append(plan.filesToSend.size()).append(' ');
        for (Map.Entry<String, List<Integer>> entry : plan.filesToSend.entrySet()) {
            message.append(entry.getKey()).append(' ').append(entry.getValue().size()).append(' ');
            entry.getValue().forEach(port -> message.append(port).append(' '));
        }

        message.append(plan.filesToDelete.size()).append(' ');
        plan.filesToDelete.forEach(file -> message.append(file).append(' '));
        System.out.println("[Controller]->[Store] rebalance plan");
        store.writer.println(message.toString().trim());
    }

}