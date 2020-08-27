use crate::{
    generator::{self, Generator},
    Error, Result,
};

use std::sync::Arc;

use fasthash::xx;

use rand::{distributions::Alphanumeric, rngs::SmallRng, Rng, SeedableRng};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum DistributionSpec {
    Constant(usize),
    Uniform(usize, usize),
    Zipfian(usize, f64),
    Latest,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkloadSpec {
    table: String,
    field_count: usize,

    field_len_dist: DistributionSpec,

    read_all_fields: bool,
    write_all_fields: bool,

    ordered_insert: bool,

    read_proportion: f64,
    update_proportion: f64,
    insert_proportion: f64,
    scan_proportion: f64,
    rmw_proportion: f64,

    request_dist: DistributionSpec,

    scan_len_dist: DistributionSpec,

    insert_start: usize,
    record_count: usize,
    operation_count: usize,
}

impl Default for WorkloadSpec {
    fn default() -> Self {
        WorkloadSpec {
            table: "usertable".to_owned(),
            field_count: 10,

            field_len_dist: DistributionSpec::Constant(100),

            read_all_fields: true,
            write_all_fields: false,

            ordered_insert: false,

            read_proportion: 0.95,
            update_proportion: 0.05,
            insert_proportion: 0.0,
            scan_proportion: 0.0,
            rmw_proportion: 0.0,

            request_dist: DistributionSpec::Uniform(1, 1000),

            scan_len_dist: DistributionSpec::Uniform(1, 1000),

            insert_start: 0,
            record_count: 0,
            operation_count: 0,
        }
    }
}

impl WorkloadSpec {
    pub fn field_count(mut self, count: usize) -> Self {
        self.field_count = count;
        self
    }

    pub fn field_len_const(mut self, len: usize) -> Self {
        self.field_len_dist = DistributionSpec::Constant(len);
        self
    }

    pub fn read_all_fields(mut self, val: bool) -> Self {
        self.read_all_fields = val;
        self
    }

    pub fn write_all_fields(mut self, val: bool) -> Self {
        self.write_all_fields = val;
        self
    }

    pub fn read_proportion(mut self, val: f64) -> Self {
        self.read_proportion = val;
        self
    }

    pub fn update_proportion(mut self, val: f64) -> Self {
        self.update_proportion = val;
        self
    }

    pub fn insert_proportion(mut self, val: f64) -> Self {
        self.insert_proportion = val;
        self
    }

    pub fn scan_proportion(mut self, val: f64) -> Self {
        self.scan_proportion = val;
        self
    }

    pub fn rmw_proportion(mut self, val: f64) -> Self {
        self.rmw_proportion = val;
        self
    }

    pub fn request_zipfian(mut self, s: f64) -> Self {
        self.request_dist = DistributionSpec::Zipfian(0, s);
        self
    }

    pub fn record_count(mut self, val: usize) -> Self {
        self.record_count = val;
        self
    }

    pub fn get_record_count(&self) -> usize {
        self.record_count
    }

    pub fn operation_count(mut self, val: usize) -> Self {
        self.operation_count = val;
        self
    }

    pub fn get_operation_count(&self) -> usize {
        self.operation_count
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    Insert,
    Read,
    Update,
    Scan,
    ReadModifyWrite,
}

impl Default for Operation {
    fn default() -> Self {
        Self::Insert
    }
}

pub struct CoreWorkload {
    field_len_generator: Box<dyn Generator<usize>>,
    op_generator:
        generator::DistributionGenerator<Operation, generator::DiscreteDistribution<Operation>>,
    key_generator: generator::CounterGenerator,
    key_sampler: Box<dyn Generator<usize>>,
    field_generator: generator::DistributionGenerator<usize, rand::distributions::Uniform<usize>>,
    scan_len_generator: Box<dyn Generator<usize>>,
    insert_key_sequence: Arc<generator::CounterGenerator>,

    field_count: usize,

    table: String,

    read_all_fields: bool,
    write_all_fields: bool,

    ordered_insert: bool,
}

impl CoreWorkload {
    pub fn new(spec: WorkloadSpec) -> Result<Self> {
        let field_len_generator: Box<dyn Generator<usize>> = match spec.field_len_dist {
            DistributionSpec::Constant(c) => Box::new(generator::ConstGenerator::new(c)),
            DistributionSpec::Uniform(min, max) => Box::new(generator::uniform_gen(min, max)),
            _ => {
                return Err(Error::InvalidArgument(
                    "field length distribution".to_owned(),
                ))
            }
        };

        let mut ops = Vec::new();
        if spec.read_proportion > 0.0 {
            ops.push((Operation::Read, spec.read_proportion));
        }
        if spec.update_proportion > 0.0 {
            ops.push((Operation::Update, spec.update_proportion));
        }
        if spec.insert_proportion > 0.0 {
            ops.push((Operation::Insert, spec.insert_proportion));
        }
        if spec.scan_proportion > 0.0 {
            ops.push((Operation::Scan, spec.scan_proportion));
        }
        if spec.rmw_proportion > 0.0 {
            ops.push((Operation::ReadModifyWrite, spec.rmw_proportion));
        }
        let op_generator = generator::discrete_gen(ops);

        let key_generator = generator::CounterGenerator::new(spec.insert_start as u64);

        let insert_key_sequence =
            Arc::new(generator::CounterGenerator::new(spec.record_count as u64));

        let key_sampler: Box<dyn Generator<usize>> = match spec.request_dist {
            DistributionSpec::Uniform(_, _) => {
                Box::new(generator::uniform_gen(0, spec.record_count - 1))
            }
            DistributionSpec::Zipfian(_, s) => Box::new(generator::zipfian_gen(
                spec.record_count
                    + (spec.operation_count as f64 * spec.insert_proportion) as usize * 2,
                s,
            )?),
            DistributionSpec::Latest => Box::new(generator::SkewedLatestGenerator::new(
                insert_key_sequence.clone(),
            )),
            _ => {
                return Err(Error::InvalidArgument(
                    "field length distribution".to_owned(),
                ))
            }
        };

        let field_generator = generator::uniform_gen(0, spec.field_count - 1);

        let scan_len_generator: Box<dyn Generator<usize>> = match spec.scan_len_dist {
            DistributionSpec::Uniform(min, max) => Box::new(generator::uniform_gen(min, max)),
            DistributionSpec::Zipfian(num_elements, s) => {
                Box::new(generator::zipfian_gen(num_elements, s)?)
            }
            _ => {
                return Err(Error::InvalidArgument(
                    "scan length distribution".to_owned(),
                ))
            }
        };

        Ok(Self {
            field_len_generator,
            op_generator,
            key_generator,
            key_sampler,
            field_generator,
            scan_len_generator,
            insert_key_sequence,

            field_count: spec.field_count,

            table: spec.table,

            read_all_fields: spec.read_all_fields,
            write_all_fields: spec.write_all_fields,

            ordered_insert: spec.ordered_insert,
        })
    }

    pub fn read_all_fields(&self) -> bool {
        self.read_all_fields
    }

    pub fn write_all_fields(&self) -> bool {
        self.write_all_fields
    }

    pub fn next_table(&self) -> String {
        self.table.clone()
    }

    pub fn next_operation(&self) -> Operation {
        self.op_generator.next()
    }

    fn get_key_name(&self, key_num: usize) -> String {
        format!(
            "user{}",
            if self.ordered_insert {
                key_num as u64
            } else {
                let ip: *const usize = &key_num;
                let bp: *const u8 = ip as *const _;
                let bs: &[u8] =
                    unsafe { std::slice::from_raw_parts(bp, std::mem::size_of::<usize>()) };

                xx::hash64(bs)
            }
        )
    }

    pub fn next_sequence_key(&self) -> String {
        self.get_key_name(self.key_generator.next() as usize)
    }

    pub fn next_insert_sequence(&self) -> String {
        self.get_key_name(self.insert_key_sequence.next() as usize)
    }

    pub fn next_transaction_key(&self) -> String {
        self.get_key_name(self.key_sampler.next())
    }

    pub fn next_field_value(&self) -> String {
        let rng = SmallRng::from_rng(rand::thread_rng()).unwrap();

        rng.sample_iter(&Alphanumeric)
            .take(self.field_len_generator.next())
            .collect::<String>()
    }

    pub fn next_scan_length(&self) -> usize {
        self.scan_len_generator.next()
    }

    pub fn build_values(&self) -> Vec<(String, String)> {
        (0..self.field_count)
            .map(|i| (format!("field{}", i), self.next_field_value()))
            .collect::<Vec<_>>()
    }

    pub fn next_field_name(&self) -> String {
        format!("field{}", self.field_generator.next())
    }

    pub fn build_update(&self) -> (String, String) {
        (self.next_field_name(), self.next_field_value())
    }
}

unsafe impl Sync for CoreWorkload {}
unsafe impl Send for CoreWorkload {}
