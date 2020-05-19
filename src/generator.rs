use crate::{Error, Result};

use std::{marker::PhantomData, sync::atomic::AtomicU64};

use rand::{
    distributions::{Distribution, Uniform},
    Rng,
};

use zipf::ZipfDistribution;

pub trait Generator<T> {
    fn next(&self) -> T;
}

pub struct ConstGenerator<T> {
    val: T,
}

impl<T> ConstGenerator<T> {
    pub fn new(val: T) -> Self {
        Self { val }
    }
}

impl<T> Generator<T> for ConstGenerator<T>
where
    T: Clone,
{
    fn next(&self) -> T {
        self.val.clone()
    }
}

pub struct DistributionGenerator<T, D: Distribution<T>> {
    dist: D,
    value_type: PhantomData<T>,
}

impl<D, T> Generator<T> for DistributionGenerator<T, D>
where
    T: Clone,
    D: Distribution<T>,
{
    fn next(&self) -> T {
        let val = self.dist.sample(&mut rand::thread_rng());
        val
    }
}

pub fn uniform_gen<T>(min: T, max: T) -> DistributionGenerator<T, Uniform<T>>
where
    T: rand::distributions::uniform::SampleUniform,
{
    DistributionGenerator {
        dist: Uniform::new(min, max),
        value_type: PhantomData,
    }
}

pub fn zipfian_gen(
    num_elements: usize,
    exponent: f64,
) -> Result<DistributionGenerator<usize, ZipfDistribution>> {
    let dist = ZipfDistribution::new(num_elements, exponent)
        .map_err(|_| Error::InvalidArgument("zipf distribution".to_owned()))?;

    Ok(DistributionGenerator {
        dist,
        value_type: PhantomData,
    })
}

pub struct DiscreteDistribution<T> {
    values: Vec<(T, f64)>,
    sum: f64,
}

impl<T> DiscreteDistribution<T> {
    fn new(values: Vec<(T, f64)>) -> Self {
        let sum = values.iter().map(|x| x.1).sum();

        Self { values, sum }
    }
}

impl<T> Distribution<T> for DiscreteDistribution<T>
where
    T: Clone + Default,
{
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> T {
        let val: f64 = rng.gen();

        let mut acc = 0.0;
        for (t, weight) in &self.values {
            acc += weight / self.sum;

            if val < acc {
                return t.clone();
            }
        }

        Default::default()
    }
}

pub fn discrete_gen<T: Clone + Default>(
    values: Vec<(T, f64)>,
) -> DistributionGenerator<T, DiscreteDistribution<T>> {
    DistributionGenerator {
        dist: DiscreteDistribution::new(values),
        value_type: PhantomData,
    }
}

pub struct CounterGenerator {
    counter: AtomicU64,
}

impl CounterGenerator {
    pub fn new(val: u64) -> Self {
        Self {
            counter: AtomicU64::new(val),
        }
    }
}
impl Generator<u64> for CounterGenerator {
    fn next(&self) -> u64 {
        self.counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_const_generator() {
        let gen = ConstGenerator::new(100);

        assert_eq!(gen.next(), 100);
    }

    #[test]
    fn test_counter_generator() {
        let gen = CounterGenerator::new(0);

        for i in 0..10 {
            assert_eq!(gen.next(), i);
        }
    }
}
