package com.weather.central.bitcask;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.stream.Collectors;

public class BitCask {

    // In-memory keydir: key -> (fileId, offset, size)
    private final ConcurrentHashMap<String, KeyDirEntry> keyDir = new ConcurrentHashMap<>();
    private final String dataDir;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // Active segment file
    private RandomAccessFile activeFile;
    private long activeFileId;

    private static final long MAX_SEGMENT_SIZE = 10 * 1024 * 1024; // 10MB per segment
    private static final String DATA_EXT = ".data";
    private static final String HINT_EXT = ".hint";

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public BitCask(String dataDir) throws IOException {
        this.dataDir = dataDir;
        Files.createDirectories(Paths.get(dataDir));
        rehash(); // recover from existing files on startup
        openNewSegment();
        scheduleCompaction();
    }

    // ── Write ────────────────────────────────────────────────────────────────
    public void put(String key, String value) throws IOException {
        byte[] keyBytes = key.getBytes();
        byte[] valueBytes = value.getBytes();
        int recordSize = 4 + 4 + keyBytes.length + valueBytes.length;

        lock.writeLock().lock();
        try {
            // Roll over to a new segment if needed
            if (activeFile.length() + recordSize >= MAX_SEGMENT_SIZE) {
                long oldFileId = activeFileId;
                activeFile.close();
                // Write hint file for the closed segment
                writeHintFile(oldFileId);
                openNewSegment();
            }

            long offset = activeFile.length();
            activeFile.seek(offset);
            activeFile.writeInt(keyBytes.length);
            activeFile.writeInt(valueBytes.length);
            activeFile.write(keyBytes);
            activeFile.write(valueBytes);

            keyDir.put(key, new KeyDirEntry(activeFileId, offset, recordSize));
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ── Read ─────────────────────────────────────────────────────────────────
    public String get(String key) throws IOException {
        lock.readLock().lock();
        try {
            KeyDirEntry entry = keyDir.get(key);
            if (entry == null) return null;

            String filePath = dataDir + "/" + entry.fileId + DATA_EXT;
            File file = new File(filePath);
            if (!file.exists()) return null;

            try (RandomAccessFile f = new RandomAccessFile(file, "r")) {
                f.seek(entry.offset);
                int keyLen = f.readInt();
                int valueLen = f.readInt();
                f.skipBytes(keyLen); // skip key bytes
                byte[] valueBytes = new byte[valueLen];
                f.readFully(valueBytes);
                return new String(valueBytes);
            } catch (FileNotFoundException e) {
                // Might happen during compaction if file was deleted but keyDir not yet updated
                return null;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    // ── Get all keys ─────────────────────────────────────────────────────────
    public Set<String> keys() {
        lock.readLock().lock();
        try {
            return Collections.unmodifiableSet(new HashSet<>(keyDir.keySet()));
        } finally {
            lock.readLock().unlock();
        }
    }

    public Map<String, String> getAll() throws IOException {
        lock.readLock().lock();
        try {
            Map<String, String> result = new HashMap<>();

            // Group keys by fileId to minimize file openings
            Map<Long, List<Map.Entry<String, KeyDirEntry>>> groupedByFile = keyDir.entrySet().stream()
                    .collect(Collectors.groupingBy(e -> e.getValue().fileId));

            for (Map.Entry<Long, List<Map.Entry<String, KeyDirEntry>>> fileEntry : groupedByFile.entrySet()) {
                long fileId = fileEntry.getKey();
                List<Map.Entry<String, KeyDirEntry>> entries = fileEntry.getValue();

                String filePath = dataDir + "/" + fileId + DATA_EXT;
                File file = new File(filePath);
                if (!file.exists()) continue;

                try (RandomAccessFile f = new RandomAccessFile(file, "r")) {
                    for (Map.Entry<String, KeyDirEntry> e : entries) {
                        KeyDirEntry kde = e.getValue();
                        f.seek(kde.offset);
                        int keyLen = f.readInt();
                        int valueLen = f.readInt();
                        f.skipBytes(keyLen);
                        byte[] valueBytes = new byte[valueLen];
                        f.readFully(valueBytes);
                        result.put(e.getKey(), new String(valueBytes));
                    }
                } catch (FileNotFoundException e) {
                    // Segment deleted during compaction; ignore and continue
                }
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    // ── Rehash (recovery) ────────────────────────────────────────────────────
    private void rehash() throws IOException {
        File dir = new File(dataDir);
        File[] dataFiles = dir.listFiles((d, n) -> n.endsWith(DATA_EXT));
        if (dataFiles == null || dataFiles.length == 0) return;

        // Sort by fileId (timestamp) to ensure latest values win
        Arrays.sort(dataFiles, Comparator.comparingLong(f -> Long.parseLong(f.getName().replace(DATA_EXT, ""))));

        for (File df : dataFiles) {
            long fileId = Long.parseLong(df.getName().replace(DATA_EXT, ""));
            File hf = new File(dataDir + "/" + fileId + HINT_EXT);
            if (hf.exists()) {
                loadFromHintFile(hf, fileId);
            } else {
                loadFromDataFile(df, fileId);
            }
        }
        System.out.println("BitCask rehash complete. Keys loaded: " + keyDir.size());
    }

    private void loadFromHintFile(File hf, long fileId) throws IOException {
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(hf)))) {
            while (dis.available() > 0) {
                int keyLen = dis.readInt();
                long offset = dis.readLong();
                int size = dis.readInt();
                byte[] keyB = new byte[keyLen];
                dis.readFully(keyB);
                keyDir.put(new String(keyB), new KeyDirEntry(fileId, offset, size));
            }
        }
    }

    private void loadFromDataFile(File df, long fileId) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(df, "r")) {
            long len = raf.length();
            while (raf.getFilePointer() < len) {
                long offset = raf.getFilePointer();
                int keyLen = raf.readInt();
                int valueLen = raf.readInt();
                byte[] keyB = new byte[keyLen];
                raf.readFully(keyB);
                raf.skipBytes(valueLen);
                int size = 4 + 4 + keyLen + valueLen;
                keyDir.put(new String(keyB), new KeyDirEntry(fileId, offset, size));
            }
        }
    }

    // ── Write hint file for a segment ───────────────────────────────
    private void writeHintFile(long fileId) throws IOException {
        String hintPath = dataDir + "/" + fileId + HINT_EXT;
        // We only want to include keys that are CURRENTLY in this segment
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(hintPath)))) {
            for (Map.Entry<String, KeyDirEntry> e : keyDir.entrySet()) {
                if (e.getValue().fileId == fileId) {
                    byte[] keyBytes = e.getKey().getBytes();
                    dos.writeInt(keyBytes.length);
                    dos.writeLong(e.getValue().offset);
                    dos.writeInt(e.getValue().size);
                    dos.write(keyBytes);
                }
            }
        }
    }

    private void writeHintFileFromEntries(long fileId, Map<String, KeyDirEntry> entries) throws IOException {
        String hintPath = dataDir + "/" + fileId + HINT_EXT;
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(hintPath)))) {
            for (Map.Entry<String, KeyDirEntry> e : entries.entrySet()) {
                byte[] keyBytes = e.getKey().getBytes();
                dos.writeInt(keyBytes.length);
                dos.writeLong(e.getValue().offset);
                dos.writeInt(e.getValue().size);
                dos.write(keyBytes);
            }
        }
    }

    // ── Open a brand-new active segment ──────────────────────────────────────
    private void openNewSegment() throws IOException {
        activeFileId = System.currentTimeMillis();
        String activeFilePath = dataDir + "/" + activeFileId + DATA_EXT;
        activeFile = new RandomAccessFile(activeFilePath, "rw");
        System.out.println("BitCask opened new segment: " + activeFilePath);
    }

    // ── Compaction ───────────────────────────────────────────────────────────
    private void scheduleCompaction() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                compact();
            } catch (Exception e) {
                System.err.println("Compaction error: ");
                e.printStackTrace();
            }
        }, 1, 5, TimeUnit.MINUTES);
    }

    public void compact() throws IOException {
        System.out.println("BitCask compaction started...");

        // 1. Identify segments to compact (all except current active)
        long currentActiveId;
        lock.readLock().lock();
        try {
            currentActiveId = activeFileId;
        } finally {
            lock.readLock().unlock();
        }

        File dir = new File(dataDir);
        File[] dataFiles = dir.listFiles((d, n) -> n.endsWith(DATA_EXT));
        if (dataFiles == null) return;

        List<Long> segmentsToCompact = Arrays.stream(dataFiles)
                .map(f -> Long.parseLong(f.getName().replace(DATA_EXT, "")))
                .filter(id -> id < currentActiveId)
                .sorted()
                .collect(Collectors.toList());

        if (segmentsToCompact.isEmpty()) {
            System.out.println("Nothing to compact.");
            return;
        }

        long maxOldId = segmentsToCompact.get(segmentsToCompact.size() - 1);
        long compactedFileId = maxOldId + 1;
        while (Files.exists(Paths.get(dataDir + "/" + compactedFileId + DATA_EXT))
                || Files.exists(Paths.get(dataDir + "/" + compactedFileId + HINT_EXT))) {
            compactedFileId++;
        }

        String compactedPath = dataDir + "/" + compactedFileId + ".compact" + DATA_EXT;
        Map<String, KeyDirEntry> compactedEntries = new HashMap<>();

        Map<Long, List<Map.Entry<String, KeyDirEntry>>> groupedByFile;
        lock.readLock().lock();
        try {
            groupedByFile = keyDir.entrySet().stream()
                    .filter(e -> segmentsToCompact.contains(e.getValue().fileId))
                    .collect(Collectors.groupingBy(e -> e.getValue().fileId));
        } finally {
            lock.readLock().unlock();
        }

        try (RandomAccessFile newFile = new RandomAccessFile(compactedPath, "rw")) {
            for (Map.Entry<Long, List<Map.Entry<String, KeyDirEntry>>> fileEntry : groupedByFile.entrySet()) {
                long fileId = fileEntry.getKey();
                String filePath = dataDir + "/" + fileId + DATA_EXT;
                File file = new File(filePath);
                if (!file.exists()) continue;

                try (RandomAccessFile oldFile = new RandomAccessFile(file, "r")) {
                    for (Map.Entry<String, KeyDirEntry> e : fileEntry.getValue()) {
                        String key = e.getKey();
                        KeyDirEntry entry = e.getValue();

                        oldFile.seek(entry.offset);
                        int keyLen = oldFile.readInt();
                        int valueLen = oldFile.readInt();
                        oldFile.skipBytes(keyLen);
                        byte[] valB = new byte[valueLen];
                        oldFile.readFully(valB);

                        byte[] keyB = key.getBytes();
                        long offset = newFile.getFilePointer();
                        newFile.writeInt(keyB.length);
                        newFile.writeInt(valB.length);
                        newFile.write(keyB);
                        newFile.write(valB);
                        int size = 4 + 4 + keyB.length + valB.length;
                        compactedEntries.put(key, new KeyDirEntry(compactedFileId, offset, size));
                    }
                }
            }
        }

        if (compactedEntries.isEmpty()) {
            Files.deleteIfExists(Paths.get(compactedPath));
            System.out.println("Compaction resulted in no keys.");
        } else {
            File compactedFile = new File(compactedPath);
            File finalFile = new File(dataDir + "/" + compactedFileId + DATA_EXT);
            if (compactedFile.renameTo(finalFile)) {
                lock.writeLock().lock();
                try {
                    for (Map.Entry<String, KeyDirEntry> e : compactedEntries.entrySet()) {
                        String key = e.getKey();
                        KeyDirEntry newEntry = e.getValue();
                        keyDir.computeIfPresent(key, (k, oldEntry) -> {
                            if (segmentsToCompact.contains(oldEntry.fileId)) {
                                return newEntry;
                            }
                            return oldEntry;
                        });
                    }
                    writeHintFileFromEntries(compactedFileId, compactedEntries);

                    for (Long id : segmentsToCompact) {
                        Files.deleteIfExists(Paths.get(dataDir + "/" + id + DATA_EXT));
                        Files.deleteIfExists(Paths.get(dataDir + "/" + id + HINT_EXT));
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }

        System.out.println("BitCask compaction done. Keys: " + keyDir.size());
    }

    public void close() throws IOException {
        scheduler.shutdown();
        lock.writeLock().lock();
        try {
            if (activeFile != null) {
                writeHintFile(activeFileId);
                activeFile.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ── KeyDir Entry ─────────────────────────────────────────────────────────
    public static class KeyDirEntry {
        public final long fileId;
        public final long offset;
        public final int size;

        public KeyDirEntry(long fileId, long offset, int size) {
            this.fileId = fileId;
            this.offset = offset;
            this.size = size;
        }
    }
}
