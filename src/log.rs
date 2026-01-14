use std::{collections::HashMap, fs::OpenOptions, io::{self, Read, Write}, sync::Mutex};
use whitewater::CompleteLogEntry;

pub struct RaftLog {
    pub term: u64,
    /// Protects both the commit index and the log file
    commit_index: Mutex<u64>,
    log: Option<Vec<CompleteLogEntry>>
}

impl RaftLog {
    pub(crate) fn new() -> Self {
        
        Self {
            term: 0,
            commit_index: Mutex::new(0),
            log: None
        }
    }

    /// returns term, commit index, map
    pub async fn load(&mut self) -> (u64, u64, HashMap<String, String>) {
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
            self.log = Some(logs);
        }

        (self.term, *index, map)
    }

    // pub async fn run(mut self) {
    //     loop {
    //         match self.queue.recv().await {
    //             Some(LogRpc::Write { entry, response }) => {
    //                 match self.write_log(entry.key, entry.value) {
    //                     Ok(entry) => response.send(entry).unwrap(),
    //                     Err(e) => panic!("{e}")
    //                 };
    //             },
    //             Some(LogRpc::Get { from, response }) => {
    //                 match self.log {
    //                     None => {
    //                         panic!("Log isn't loaded");
    //                     }
    //                     Some(ref log) => {
    //                         // don't copy here (but the whole program probably shouldn't use message passing ugh)
    //                         response.send(log.iter().filter(|e| (**e).index <= from).map(|e| e.clone()).collect());
    //                     }
    //                 };
    //             }
    //             None => {
    //                 break;
    //             }
    //         }
            
    //     }
    // }

    /// Writes a KV pair to disk, appends a log entry for it to the queue, and returns its commit index
    pub fn write_log(&self, key: String, value: String) -> io::Result<CompleteLogEntry> {
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