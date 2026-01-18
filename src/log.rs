use std::{collections::HashMap, fs::OpenOptions, io::{self, Read, Write}};
use whitewater::CompleteLogEntry;

pub struct RaftLog {
    pub term: u64,
    pub commit_index: u64,
    pub map: HashMap<String,String>,
    log: Vec<CompleteLogEntry>
}

impl RaftLog {

    pub(crate) fn new() -> Self {
        // this is probably doing too much

        let mut term = 0;
        let mut commit_index = 0;
        let mut map = HashMap::new();
        let mut log = vec![];

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
                        if term < entry.term {
                            term = entry.term
                        }

                        if commit_index < entry.index {
                            commit_index = entry.index;
                        }
                        log.push(entry);
                    }
                    _ => {}
                }
            }

            println!("Loaded {} entries, term {}, index {}", log.len(), term, commit_index);
        }

        
        Self {
            term,
            commit_index,
            map: HashMap::new(),
            log
        }
    }

    /// Writes a KV pair to disk, appends a log entry for it to the queue, and returns its commit index
    pub fn write_log(&mut self, key: String, value: String) -> io::Result<CompleteLogEntry> {
        self.commit_index += 1;

        let mut log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("raft.log")?;

        let log = CompleteLogEntry {
            term: self.term,
            index: self.commit_index,
            key: key,
            value: value
        };

        let buf = rmp_serde::to_vec(&log).unwrap();

        log_file.write_all(&buf)?;

        // is this copy necessary?
        self.map.insert(log.key.clone(), log.value.clone());
        Ok(log)
    }
}