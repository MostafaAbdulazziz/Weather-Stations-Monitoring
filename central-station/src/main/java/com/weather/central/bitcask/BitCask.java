package com.weather.central.bitcask;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public class BitCask {

    // In-memory keydir: key -> (fileId, offset, size)
    private final Map<String, KeyDirEntry> keyDir = new ConcurrentHashMap<>();
    private final String dataDir;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // Active segment file
    private RandomAccessFile activeFile;
    private String activeFilePath;
    private long activeFileId;

    private static final long MAX_SEGMENT_SIZE = 10 * 1024 * 1024; // 10MB per segment
    private static final String DATA_EXT  = ".data";
    private static final String HINT_EXT  = ".hint";

    public BitCask(String dataDir) throws IOException {
        this.dataDir = dataDir;
        Files.createDirectories(Paths.get(dataDir));
        rehash(); // recover from existing files on startup
        openNewSegment();
        scheduleCompaction();
    }

    // ── Write ────────────────────────────────────────────────────────────────
    public void put(String key, String value) throws IOException {
        lock.writeLock().lock();
        try {
            byte[] keyBytes   = key.getBytes();
            byte[] valueBytes = value.getBytes();

            // Roll over to a new segment if needed
            if (activeFile.length() >= MAX_SEGMENT_SIZE) {
                writeHintFile(activeFilePath, activeFileId);
                activeFile.close();
                openNewSegment();
            }

            long offset = activeFile.length();

            // Format: [keyLen(4)][valueLen(4)][key][value]
            activeFile.seek(offset);
            activeFile.writeInt(keyBytes.length);
            activeFile.writeInt(valueBytes.length);
            activeFile.write(keyBytes);
            activeFile.write(valueBytes);

            keyDir.put(key, new KeyDirEntry(activeFileId, offset,
                    4 + 4 + keyBytes.length + valueBytes.length));
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
            try (RandomAccessFile f = new RandomAccessFile(filePath, "r")) {
                f.seek(entry.offset);
                int keyLen   = f.readInt();
                int valueLen = f.readInt();
                f.skipBytes(keyLen);                  // skip key bytes
                byte[] valueBytes = new byte[valueLen];
                f.readFully(valueBytes);
                return new String(valueBytes);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    // ── Get all keys ─────────────────────────────────────────────────────────
    public Set<String> keys() {
        return Collections.unmodifiableSet(keyDir.keySet());
    }

    // ── Rehash (recovery) ────────────────────────────────────────────────────
    private void rehash() throws IOException {
        File dir = new File(dataDir);
        File[] hintFiles = dir.listFiles((d, n) -> n.endsWith(HINT_EXT));
        if (hintFiles == null) return;

        Arrays.sort(hintFiles); // process in order
        for (File hf : hintFiles) {
            long fileId = Long.parseLong(
                hf.getName().replace(HINT_EXT, ""));
            try (DataInputStream dis = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(hf)))) {
                while (dis.available() > 0) {
                    int    keyLen = dis.readInt();
                    long   offset = dis.readLong();
                    int    size   = dis.readInt();
                    byte[] keyB   = new byte[keyLen];
                    dis.readFully(keyB);
                    keyDir.put(new String(keyB),
                               new KeyDirEntry(fileId, offset, size));
                }
            }
        }
        System.out.println("BitCask rehash complete. Keys loaded: " + keyDir.size());
    }

    // ── Write hint file for a completed segment ───────────────────────────────
    private void writeHintFile(String dataFilePath, long fileId) throws IOException {
        String hintPath = dataDir + "/" + fileId + HINT_EXT;
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(hintPath)))) {
            for (Map.Entry<String, KeyDirEntry> e : keyDir.entrySet()) {
                if (e.getValue().fileId != fileId) continue;
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
        activeFileId   = System.currentTimeMillis();
        activeFilePath = dataDir + "/" + activeFileId + DATA_EXT;
        activeFile     = new RandomAccessFile(activeFilePath, "rw");
        System.out.println("BitCask opened new segment: " + activeFilePath);
    }

    // ── Compaction ───────────────────────────────────────────────────────────
    private void scheduleCompaction() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try { compact(); }
            catch (Exception e) {
                System.err.println("Compaction error: " + e.getMessage());
            }
        }, 5, 5, TimeUnit.MINUTES);
    }

    private void compact() throws IOException {
        lock.writeLock().lock();
        try {
            System.out.println("BitCask compaction started...");
            long newFileId   = System.currentTimeMillis();
            String newPath   = dataDir + "/" + newFileId + ".compact" + DATA_EXT;
            Map<String, KeyDirEntry> newKeyDir = new HashMap<>();

            try (RandomAccessFile newFile = new RandomAccessFile(newPath, "rw")) {
                for (String key : keyDir.keySet()) {
                    String value = get(key);
                    if (value == null) continue;

                    byte[] keyB = key.getBytes();
                    byte[] valB = value.getBytes();
                    long offset = newFile.length();
                    newFile.seek(offset);
                    newFile.writeInt(keyB.length);
                    newFile.writeInt(valB.length);
                    newFile.write(keyB);
                    newFile.write(valB);
                    newKeyDir.put(key, new KeyDirEntry(newFileId,
                            offset, 4 + 4 + keyB.length + valB.length));
                }
            }

            // Rename compacted file to proper .data
            File compacted = new File(newPath);
            File finalFile = new File(dataDir + "/" + newFileId + DATA_EXT);
            compacted.renameTo(finalFile);

            // Update keydir
            keyDir.clear();
            keyDir.putAll(newKeyDir);

            // Write hint for compacted file
            writeHintFile(finalFile.getAbsolutePath(), newFileId);

            System.out.println("BitCask compaction done. Keys: " + keyDir.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ── KeyDir Entry ─────────────────────────────────────────────────────────
    public static class KeyDirEntry {
        public final long fileId;
        public final long offset;
        public final int  size;

        public KeyDirEntry(long fileId, long offset, int size) {
            this.fileId = fileId;
            this.offset = offset;
            this.size   = size;
        }
    }
}
