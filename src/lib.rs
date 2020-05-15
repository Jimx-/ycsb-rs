extern crate rand;
extern crate zipf;

mod generator;

pub use crate::generator::{uniform_gen, zipfian_gen, ConstGenerator, DistributionGenerator};
