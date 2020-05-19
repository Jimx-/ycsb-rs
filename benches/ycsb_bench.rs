#![feature(test)]

extern crate rand;
extern crate test;
extern crate ycsb_rs;

use ycsb_rs::{Client, CoreWorkload, Db, MockDb, WorkloadSpec};

use test::Bencher;

#[bench]
fn bench_next_sequence_key(b: &mut Bencher) {
    let spec = WorkloadSpec::default();
    let workload = CoreWorkload::new(spec).unwrap();

    b.iter(|| workload.next_sequence_key());
}

#[bench]
fn bench_next_transaction_key(b: &mut Bencher) {
    let spec = WorkloadSpec::default();
    let workload = CoreWorkload::new(spec).unwrap();

    b.iter(|| workload.next_transaction_key());
}

#[bench]
fn bench_next_zipfian_key(b: &mut Bencher) {
    let spec = WorkloadSpec::default()
        .request_zipfian(1.07)
        .record_count(10000);
    let workload = CoreWorkload::new(spec).unwrap();

    b.iter(|| workload.next_transaction_key());
}

#[bench]
fn bench_next_value(b: &mut Bencher) {
    let spec = WorkloadSpec::default();
    let workload = CoreWorkload::new(spec).unwrap();

    b.iter(|| workload.next_field_value());
}

#[bench]
fn bench_read_txn(b: &mut Bencher) {
    let spec = WorkloadSpec::default();
    let workload = CoreWorkload::new(spec).unwrap();
    let db = MockDb::new(true);
    let client = Client::new(&db, &workload);

    b.iter(|| {
        let mut txn = db.start_transaction().unwrap();
        let res = client.read_txn(&mut txn);

        match res {
            Ok(_) => {
                db.commit_transaction(txn).unwrap();
            }
            err => {
                db.abort_transaction(txn).unwrap();
            }
        }
    });
}

#[bench]
fn bench_insert_txn(b: &mut Bencher) {
    let spec = WorkloadSpec::default();
    let workload = CoreWorkload::new(spec).unwrap();
    let db = MockDb::new(true);
    let client = Client::new(&db, &workload);

    b.iter(|| {
        let mut txn = db.start_transaction().unwrap();
        let res = client.insert_txn(&mut txn);

        match res {
            Ok(_) => {
                db.commit_transaction(txn).unwrap();
            }
            _ => {
                db.abort_transaction(txn).unwrap();
            }
        }
    });
}
