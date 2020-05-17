extern crate fasthash;
extern crate indicatif;
extern crate rand;
extern crate serde;
extern crate serde_json;
extern crate zipf;

mod db;
mod generator;
mod result;
mod workload;

pub use crate::{
    db::{Db, MockDb},
    result::{Error, Result},
    workload::{CoreWorkload, Operation, WorkloadSpec},
};

use std::{fs::File, io::prelude::*, path::Path, sync::Arc, thread, time::Instant};

use indicatif::{ProgressBar, ProgressStyle};

struct Client<'a, T> {
    db: &'a dyn Db<Transaction = T>,
    workload: &'a CoreWorkload,
}

impl<T> Client<'_, T> {
    fn read_txn(&self, txn: &mut T) -> Result<()> {
        let table = self.workload.next_table();
        let key = self.workload.next_transaction_key();

        let fields = if self.workload.read_all_fields() {
            None
        } else {
            Some(vec![self.workload.next_field_name()])
        };

        self.db.read(txn, &table, &key, fields).map(|_| ())
    }

    fn update_txn(&self, txn: &mut T) -> Result<()> {
        let table = self.workload.next_table();
        let key = self.workload.next_transaction_key();

        let values = if self.workload.write_all_fields() {
            vec![self.workload.build_update()]
        } else {
            self.workload.build_values()
        };

        self.db.update(txn, &table, key, values).map(|_| ())
    }

    fn insert_txn(&self, txn: &mut T) -> Result<()> {
        let table = self.workload.next_table();
        let key = self.workload.next_sequence_key();
        let values = self.workload.build_values();

        self.db.insert(txn, &table, key, values).map(|_| ())
    }

    fn scan_txn(&self, txn: &mut T) -> Result<()> {
        let table = self.workload.next_table();
        let key = self.workload.next_transaction_key();
        let length = self.workload.next_scan_length();

        let fields = if self.workload.read_all_fields() {
            None
        } else {
            Some(vec![self.workload.next_field_name()])
        };

        self.db.scan(txn, &table, &key, length, fields).map(|_| ())
    }

    fn rmw_txn(&self, txn: &mut T) -> Result<()> {
        let table = self.workload.next_table();
        let key = self.workload.next_transaction_key();

        let fields = if self.workload.read_all_fields() {
            None
        } else {
            Some(vec![self.workload.next_field_name()])
        };

        self.db.read(txn, &table, &key, fields)?;

        let values = if self.workload.write_all_fields() {
            vec![self.workload.build_update()]
        } else {
            self.workload.build_values()
        };

        self.db.update(txn, &table, key, values).map(|_| ())
    }
}

fn load_batch<T>(
    db: &dyn Db<Transaction = T>,
    txn: &mut T,
    batch: &[(String, String, Vec<(String, String)>)],
) -> Result<usize> {
    let batch_size = batch.len();

    for (table, key, values) in batch {
        db.insert(txn, table, key.to_owned(), values.clone())?;
    }

    Ok(batch_size)
}

fn load_db<T>(
    db: &dyn Db<Transaction = T>,
    workload: &CoreWorkload,
    num_ops: usize,
    batch_size: usize,
    pb: &ProgressBar,
) -> Result<usize> {
    let mut total_count = 0;

    for b in (0..num_ops).step_by(batch_size) {
        let count = std::cmp::min(batch_size, num_ops - b);

        let batch = (0..count)
            .map(|_| {
                (
                    workload.next_table(),
                    workload.next_sequence_key(),
                    workload.build_values(),
                )
            })
            .collect::<Vec<_>>();

        loop {
            let mut txn = db.start_transaction()?;

            match load_batch(db, &mut txn, &batch) {
                Ok(count) => {
                    total_count += count;
                    pb.inc(count as u64);
                    db.commit_transaction(txn)?;
                    break;
                }
                Err(Error::TransactionAborted) => {
                    db.abort_transaction(txn)?;
                    continue;
                }
                err => {
                    db.abort_transaction(txn)?;
                    return err;
                }
            }
        }
    }

    Ok(total_count)
}

fn bench_txn<T>(
    db: &dyn Db<Transaction = T>,
    workload: &CoreWorkload,
    num_ops: usize,
    pb: &ProgressBar,
) -> Result<usize> {
    let client = Client { db, workload };
    let mut total_count = 0;

    for _ in 0..num_ops {
        let op = workload.next_operation();

        loop {
            let mut txn = db.start_transaction()?;

            let res = match op {
                Operation::Read => client.read_txn(&mut txn),
                Operation::Update => client.update_txn(&mut txn),
                Operation::Insert => client.insert_txn(&mut txn),
                Operation::Scan => client.scan_txn(&mut txn),
                Operation::ReadModifyWrite => client.rmw_txn(&mut txn),
            };

            match res {
                Ok(_) => {
                    total_count += 1;
                    pb.inc(1);
                    db.commit_transaction(txn)?;
                    break;
                }
                Err(Error::TransactionAborted) => {
                    db.abort_transaction(txn)?;
                    continue;
                }
                err => {
                    db.abort_transaction(txn)?;
                    return err.map(|_| 0);
                }
            }
        }
    }

    Ok(total_count)
}

pub fn run_ycsb<P: AsRef<Path>, T: 'static>(
    db: Arc<dyn Db<Transaction = T>>,
    workload_path: P,
    nr_threads: usize,
    seed: u64,
) -> Result<()> {
    let mut file = File::open(workload_path)?;
    let mut json_data = String::new();
    file.read_to_string(&mut json_data)?;

    let workload_spec =
        serde_json::from_str::<WorkloadSpec>(&json_data).map_err(|_| Error::UnknownSpecFormat)?;
    let record_count = workload_spec.get_record_count();
    let op_count = workload_spec.get_operation_count();
    let workload = Arc::new(CoreWorkload::new(workload_spec, seed)?);

    let sty = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:60.cyan/blue} {pos:>7}/{len:7} {per_sec}")
        .progress_chars("##-");

    {
        let pb = Arc::new(ProgressBar::new(
            (record_count / nr_threads * nr_threads) as u64,
        ));
        pb.set_style(sty.clone());

        let threads = (0..nr_threads).map(|_| {
            let db = db.clone();
            let workload = workload.clone();
            let pb = pb.clone();

            thread::spawn(move || load_db(&*db, &workload, record_count / nr_threads, 32, &*pb))
        });

        let loaded: usize = threads
            .map(|t| t.join().unwrap())
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .sum();

        pb.finish();

        eprintln!("{} records loaded", loaded);
    }

    {
        let pb = Arc::new(ProgressBar::new(
            (op_count / nr_threads * nr_threads) as u64,
        ));
        pb.set_style(sty);

        let start = Instant::now();

        let threads = (0..nr_threads).map(|_| {
            let db = db.clone();
            let workload = workload.clone();
            let pb = pb.clone();

            thread::spawn(move || bench_txn(&*db, &workload, op_count / nr_threads, &*pb))
        });

        let nr_txns: usize = threads
            .map(|t| t.join().unwrap())
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .sum();

        let elapsed = start.elapsed();

        pb.finish();

        eprintln!("{} transactions in {:?}", nr_txns, elapsed);
        eprintln!(
            "Throughput: {:.2} KTPS",
            nr_txns as f64 / elapsed.as_secs_f64() / 1000.0
        )
    }

    Ok(())
}
