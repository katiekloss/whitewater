use std::{collections::HashMap, fs::OpenOptions, io::{self, Read, Write}, sync::Mutex};

use tokio::sync::{mpsc, oneshot};
use whitewater::{CompleteLogEntry, ShortLogEntry};

pub struct LogWrite {
    pub entry: ShortLogEntry,
    pub response: oneshot::Sender<CompleteLogEntry>
}

pub struct RaftLog {
    pub term: u64,
    queue: mpsc::Receiver<LogWrite>,
    /// Protects both the commit index and the log file
    pub commit_index: Mutex<u64>
}

impl RaftLog {
    pub(crate) fn new(queue: mpsc::Receiver<LogWrite>) -> Self {
        
        Self {
            term: 0,
            queue,
            commit_index: Mutex::new(0)
        }
    }

    pub async fn load(&mut self) -> HashMap<String, String> {
        let mut index = self.commit_index.lock().unwrap();
        
        let mut map = HashMap::new();
        let mut logs = vec![];

        let log_open = OpenOptions::new()
            .read(true)
            .open("raft.log");

        if log_open.is_ok() {
            let mut log_file = log_open.unwrap();
            let mut msgpack_buf = vec![];

            // read one byte at a time (don't @ me) and attempt to deserialize what we have so far into a log entry.
            // when we read a complete entry, add it to the vector and try to do it again, until we reach the end of the file.
            loop {
                let mut buf = vec![0; 1];
                match log_file.read(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => msgpack_buf.append(&mut buf),
                    Err(e) => panic!("{e}")
                };
                
                match rmp_serde::from_slice::<CompleteLogEntry>(&msgpack_buf) {
                    Ok(entry) => {
                        msgpack_buf.clear();
                        map.insert(entry.key.clone(), entry.value.clone());
                        if self.term < entry.term {
                            self.term = entry.term
                        }

                        if *index < entry.index {
                            *index = entry.index;
                        }
                        logs.push(entry);
                    }
                    _ => {}
                }
            }

            println!("Loaded {} entries, term {}, index {}", logs.len(), self.term, *index);
        }

        map
    }

    pub async fn run(mut self) {
        loop {
            match self.queue.recv().await {
                Some(write) => {
                    match self.write_log(write.entry.key, write.entry.value) {
                        Ok(entry) => write.response.send(entry).unwrap(),
                        Err(e) => panic!("{e}")
                    };
                },
                None => {
                    break;
                }
            }
            
        }
    }

    /// Writes a KV pair to disk, appends a log entry for it to the queue, and returns its commit index
    fn write_log(&self, key: String, value: String) -> io::Result<CompleteLogEntry> {
        let mut commit_index = self.commit_index.lock().unwrap();
        *commit_index += 1;

        let mut log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("raft.log")?;

        let log = CompleteLogEntry {
            term: self.term,
            index: *commit_index,
            key: key,
            value: value
        };

        let buf = rmp_serde::to_vec(&log).unwrap();

        log_file.write_all(&buf)?;

        Ok(log)
    }
}