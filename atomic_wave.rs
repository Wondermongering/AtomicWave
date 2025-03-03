use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use log::{info, warn, error, debug};

/// AtomicWave errors
#[derive(Error, Debug)]
pub enum AtomicWaveError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    
    #[error("Key not found: {0}")]
    KeyNotFound(String),
    
    #[error("Transaction error: {0}")]
    Transaction(String),
    
    #[error("Consistency error: {0}")]
    Consistency(String),
}

type Result<T> = std::result::Result<T, AtomicWaveError>;

/// Entry in the WAL (Write-Ahead Log)
#[derive(Serialize, Deserialize, Debug, Clone)]
struct LogEntry {
    timestamp: u64,
    operation: Operation,
    key: String,
    value: Option<Vec<u8>>,
}

/// Operations that can be performed on the store
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
enum Operation {
    Put,
    Delete,
    BeginTransaction,
    CommitTransaction,
    AbortTransaction,
}

/// Configuration for the AtomicWave store
#[derive(Debug, Clone)]
pub struct Config {
    data_dir: PathBuf,
    sync_writes: bool,
    max_log_size: usize,
    compaction_threshold: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            data_dir: PathBuf::from("./atomic_wave_data"),
            sync_writes: true,
            max_log_size: 1024 * 1024 * 10, // 10MB
            compaction_threshold: 1000,      // Number of entries before compaction
        }
    }
}

/// The main AtomicWave key-value store
pub struct AtomicWave {
    config: Config,
    store: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    log_file: Arc<Mutex<File>>,
    log_entries: Arc<RwLock<Vec<LogEntry>>>,
    transaction_in_progress: Arc<RwLock<bool>>,
}

impl AtomicWave {
    /// Create a new AtomicWave store with the given configuration
    pub fn new(config: Config) -> Result<Self> {
        fs::create_dir_all(&config.data_dir)?;
        
        let log_path = config.data_dir.join("wal.log");
        let store_path = config.data_dir.join("store.bin");
        
        // Initialize the in-memory store
        let store = Arc::new(RwLock::new(HashMap::new()));
        
        // Open or create the log file
        let log_file = Arc::new(Mutex::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)?
        ));
        
        // Load existing data if available
        let log_entries = if Path::new(&store_path).exists() {
            // Load from snapshot
            let mut file = File::open(&store_path)?;
            let mut data = Vec::new();
            file.read_to_end(&mut data)?;
            
            if !data.is_empty() {
                let loaded_store: HashMap<String, Vec<u8>> = bincode::deserialize(&data)?;
                *store.write().unwrap() = loaded_store;
            }
            
            // Load any entries from WAL that occurred after the snapshot
            Arc::new(RwLock::new(Self::load_log_entries(&log_path)?))
        } else {
            // Just load from WAL
            let entries = Self::load_log_entries(&log_path)?;
            Arc::new(RwLock::new(entries))
        };
        
        // Replay the log to recover state
        let mut store_guard = store.write().unwrap();
        for entry in log_entries.read().unwrap().iter() {
            match entry.operation {
                Operation::Put => {
                    if let Some(value) = &entry.value {
                        store_guard.insert(entry.key.clone(), value.clone());
                    }
                },
                Operation::Delete => {
                    store_guard.remove(&entry.key);
                },
                _ => {} // Transaction markers don't affect the store directly
            }
        }
        drop(store_guard);
        
        let kv_store = AtomicWave {
            config,
            store,
            log_file,
            log_entries,
            transaction_in_progress: Arc::new(RwLock::new(false)),
        };
        
        // Check if we should do compaction on startup
        if kv_store.log_entries.read().unwrap().len() >= kv_store.config.compaction_threshold {
            kv_store.compact()?;
        }
        
        Ok(kv_store)
    }
    
    /// Load entries from the write-ahead log
    fn load_log_entries(log_path: &Path) -> Result<Vec<LogEntry>> {
        if !log_path.exists() {
            return Ok(Vec::new());
        }
        
        let file = File::open(log_path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut buffer = Vec::new();
        
        loop {
            buffer.clear();
            let mut size_buf = [0u8; 8];
            match reader.read_exact(&mut size_buf) {
                Ok(_) => {
                    let entry_size = u64::from_le_bytes(size_buf) as usize;
                    buffer.resize(entry_size, 0);
                    reader.read_exact(&mut buffer)?;
                    
                    let entry: LogEntry = bincode::deserialize(&buffer)?;
                    entries.push(entry);
                },
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(AtomicWaveError::Io(e)),
            }
        }
        
        Ok(entries)
    }
    
    /// Write an entry to the write-ahead log
    fn write_log_entry(&self, entry: LogEntry) -> Result<()> {
        let data = bincode::serialize(&entry)?;
        let mut log_file = self.log_file.lock().unwrap();
        
        // Write size of entry followed by entry data
        log_file.write_all(&(data.len() as u64).to_le_bytes())?;
        log_file.write_all(&data)?;
        
        if self.config.sync_writes {
            log_file.sync_data()?;
        }
        
        // Add to in-memory log entries
        self.log_entries.write().unwrap().push(entry);
        
        if self.log_entries.read().unwrap().len() >= self.config.compaction_threshold {
            // Release log file lock before compaction
            drop(log_file);
            self.compact()?;
        }
        
        Ok(())
    }
    
    /// Compact the database by creating a snapshot and truncating the WAL
    pub fn compact(&self) -> Result<()> {
        debug!("Starting compaction");
        
        // Create a snapshot of current state
        let snapshot_path = self.config.data_dir.join("store.bin.tmp");
        let mut snapshot_file = File::create(&snapshot_path)?;
        let store_data = bincode::serialize(&*self.store.read().unwrap())?;
        snapshot_file.write_all(&store_data)?;
        snapshot_file.sync_data()?;
        
        // Replace old snapshot with new one
        let final_path = self.config.data_dir.join("store.bin");
        fs::rename(&snapshot_path, &final_path)?;
        
        // Create a new empty WAL
        let log_path = self.config.data_dir.join("wal.log");
        let new_log_path = self.config.data_dir.join("wal.log.new");
        
        {
            let new_log = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&new_log_path)?;
            
            // Get exclusive lock on the log file to replace it
            let mut log_file_guard = self.log_file.lock().unwrap();
            
            // Clear in-memory log entries since they're now in the snapshot
            self.log_entries.write().unwrap().clear();
            
            // Swap the log files
            *log_file_guard = new_log;
            
            if self.config.sync_writes {
                log_file_guard.sync_data()?;
            }
        }
        
        // Remove the old log
        fs::rename(&new_log_path, &log_path)?;
        
        debug!("Compaction complete");
        Ok(())
    }
    
    /// Get a value by key
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let store = self.store.read().unwrap();
        Ok(store.get(key).cloned())
    }
    
    /// Put a key-value pair
    pub fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        // Check if transaction is in progress
        if !*self.transaction_in_progress.read().unwrap() {
            // Direct put
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            
            let entry = LogEntry {
                timestamp,
                operation: Operation::Put,
                key: key.to_string(),
                value: Some(value.to_vec()),
            };
            
            // Write to WAL first
            self.write_log_entry(entry)?;
            
            // Update in-memory store
            self.store.write().unwrap().insert(key.to_string(), value.to_vec());
            
            Ok(())
        } else {
            // In transaction mode, just log the operation
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            
            let entry = LogEntry {
                timestamp,
                operation: Operation::Put,
                key: key.to_string(),
                value: Some(value.to_vec()),
            };
            
            self.write_log_entry(entry)
        }
    }
    
    /// Delete a key-value pair
    pub fn delete(&self, key: &str) -> Result<()> {
        // Check if transaction is in progress
        if !*self.transaction_in_progress.read().unwrap() {
            // Verify key exists
            if !self.store.read().unwrap().contains_key(key) {
                return Err(AtomicWaveError::KeyNotFound(key.to_string()));
            }
            
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            
            let entry = LogEntry {
                timestamp,
                operation: Operation::Delete,
                key: key.to_string(),
                value: None,
            };
            
            // Write to WAL first
            self.write_log_entry(entry)?;
            
            // Update in-memory store
            self.store.write().unwrap().remove(key);
            
            Ok(())
        } else {
            // In transaction mode, just log the operation
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            
            let entry = LogEntry {
                timestamp,
                operation: Operation::Delete,
                key: key.to_string(),
                value: None,
            };
            
            self.write_log_entry(entry)
        }
    }
    
    /// Begin a transaction
    pub fn begin_transaction(&self) -> Result<()> {
        let mut transaction_flag = self.transaction_in_progress.write().unwrap();
        
        if *transaction_flag {
            return Err(AtomicWaveError::Transaction(
                "Transaction already in progress".to_string()
            ));
        }
        
        *transaction_flag = true;
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let entry = LogEntry {
            timestamp,
            operation: Operation::BeginTransaction,
            key: String::new(),
            value: None,
        };
        
        self.write_log_entry(entry)?;
        
        Ok(())
    }
    
    /// Commit a transaction
    pub fn commit_transaction(&self) -> Result<()> {
        let mut transaction_flag = self.transaction_in_progress.write().unwrap();
        
        if !*transaction_flag {
            return Err(AtomicWaveError::Transaction(
                "No transaction in progress".to_string()
            ));
        }
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let entry = LogEntry {
            timestamp,
            operation: Operation::CommitTransaction,
            key: String::new(),
            value: None,
        };
        
        self.write_log_entry(entry)?;
        
        // Apply all pending operations since the BeginTransaction marker
        let mut start_index = 0;
        let log_entries = self.log_entries.read().unwrap();
        
        // Find the most recent BeginTransaction marker
        for (i, entry) in log_entries.iter().enumerate().rev() {
            if entry.operation == Operation::BeginTransaction {
                start_index = i;
                break;
            }
        }
        
        // Apply all operations between BeginTransaction and CommitTransaction
        let mut store = self.store.write().unwrap();
        for i in start_index + 1..log_entries.len() {
            if log_entries[i].operation == Operation::CommitTransaction {
                continue;
            }
            
            match log_entries[i].operation {
                Operation::Put => {
                    if let Some(value) = &log_entries[i].value {
                        store.insert(log_entries[i].key.clone(), value.clone());
                    }
                },
                Operation::Delete => {
                    store.remove(&log_entries[i].key);
                },
                _ => {}
            }
        }
        
        // Reset transaction flag
        *transaction_flag = false;
        
        Ok(())
    }
    
    /// Abort a transaction
    pub fn abort_transaction(&self) -> Result<()> {
        let mut transaction_flag = self.transaction_in_progress.write().unwrap();
        
        if !*transaction_flag {
            return Err(AtomicWaveError::Transaction(
                "No transaction in progress".to_string()
            ));
        }
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let entry = LogEntry {
            timestamp,
            operation: Operation::AbortTransaction,
            key: String::new(),
            value: None,
        };
        
        self.write_log_entry(entry)?;
        
        // Reset transaction flag
        *transaction_flag = false;
        
        Ok(())
    }
    
    /// Close the database, ensuring all data is flushed to disk
    pub fn close(self) -> Result<()> {
        // If a transaction is in progress, abort it
        if *self.transaction_in_progress.read().unwrap() {
            self.abort_transaction()?;
        }
        
        // Force compaction to make sure all data is saved
        self.compact()?;
        
        // Lock is automatically released when self is dropped
        Ok(())
    }
}

// Implement simple iterator over keys and values
pub struct AtomicWaveIterator {
    keys: Vec<String>,
    store: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    current: usize,
}

impl Iterator for AtomicWaveIterator {
    type Item = (String, Vec<u8>);
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.keys.len() {
            return None;
        }
        
        let key = &self.keys[self.current];
        self.current += 1;
        
        let store = self.store.read().unwrap();
        store.get(key).map(|value| (key.clone(), value.clone()))
    }
}

impl AtomicWave {
    /// Create an iterator over all key-value pairs
    pub fn iter(&self) -> AtomicWaveIterator {
        let store = self.store.read().unwrap();
        let keys: Vec<String> = store.keys().cloned().collect();
        
        AtomicWaveIterator {
            keys,
            store: self.store.clone(),
            current: 0,
        }
    }
}

// Example usage
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[test]
    fn test_basic_operations() -> Result<()> {
        let temp_dir = tempdir()?;
        
        let config = Config {
            data_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        
        let db = AtomicWave::new(config)?;
        
        // Test put and get
        db.put("key1", b"value1")?;
        db.put("key2", b"value2")?;
        
        assert_eq!(db.get("key1")?.unwrap(), b"value1");
        assert_eq!(db.get("key2")?.unwrap(), b"value2");
        assert_eq!(db.get("key3")?, None);
        
        // Test delete
        db.delete("key1")?;
        assert_eq!(db.get("key1")?, None);
        
        // Test transaction
        db.begin_transaction()?;
        db.put("key3", b"value3")?;
        db.put("key4", b"value4")?;
        
        // These changes shouldn't be visible until commit
        assert_eq!(db.get("key3")?, None);
        assert_eq!(db.get("key4")?, None);
        
        db.commit_transaction()?;
        
        // Now they should be visible
        assert_eq!(db.get("key3")?.unwrap(), b"value3");
        assert_eq!(db.get("key4")?.unwrap(), b"value4");
        
        // Test transaction abort
        db.begin_transaction()?;
        db.put("key5", b"value5")?;
        db.abort_transaction()?;
        
        // This should not be visible
        assert_eq!(db.get("key5")?, None);
        
        // Test iterator
        let mut pairs = Vec::new();
        for (k, v) in db.iter() {
            pairs.push((k, v));
        }
        
        assert_eq!(pairs.len(), 3); // key2, key3, key4
        
        // Close properly
        db.close()?;
        
        Ok(())
    }
    
    #[test]
    fn test_recovery() -> Result<()> {
        let temp_dir = tempdir()?;
        
        let config = Config {
            data_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        
        {
            let db = AtomicWave::new(config.clone())?;
            db.put("key1", b"value1")?;
            db.put("key2", b"value2")?;
            // No explicit close, simulating a crash
        }
        
        // Reopen and check if data was recovered
        let db2 = AtomicWave::new(config)?;
        assert_eq!(db2.get("key1")?.unwrap(), b"value1");
        assert_eq!(db2.get("key2")?.unwrap(), b"value2");
        
        Ok(())
    }
}
