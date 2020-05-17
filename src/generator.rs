use crate::{Error, Result};

use std::sync::{atomic::AtomicU64, Mutex};

use rand::{
    distributions::{Distribution, Uniform},
    rngs::StdRng,
    Rng, SeedableRng,
};

use zipf::ZipfDistribution;

pub trait Generator<T> {
    fn next(&self) -> T;
    fn last(&self) -> Option<T>;
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

    fn last(&self) -> Option<T> {
        Some(self.val.clone())
    }
}

pub struct DistributionGenerator<T, D: Distribution<T>> {
    state: Mutex<(Option<T>, StdRng)>,
    dist: D,
}

impl<D, T> Generator<T> for DistributionGenerator<T, D>
where
    T: Clone,
    D: Distribution<T>,
{
    fn next(&self) -> T {
        let mut guard = self.state.lock().unwrap();

        let val = self.dist.sample(&mut guard.1);
        guard.0 = Some(val.clone());
        val
    }

    fn last(&self) -> Option<T> {
        let guard = self.state.lock().unwrap();

        guard.0.clone()
    }
}

pub fn uniform_gen<T>(min: T, max: T, seed: u64) -> DistributionGenerator<T, Uniform<T>>
where
    T: rand::distributions::uniform::SampleUniform,
{
    DistributionGenerator {
        state: Mutex::new((None, StdRng::seed_from_u64(seed))),
        dist: Uniform::new(min, max),
    }
}

pub fn zipfian_gen(
    num_elements: usize,
    exponent: f64,
    seed: u64,
) -> Result<DistributionGenerator<usize, ZipfDistribution>> {
    let dist = ZipfDistribution::new(num_elements, exponent)
        .map_err(|_| Error::InvalidArgument("zipf distribution".to_owned()))?;

    Ok(DistributionGenerator {
        state: Mutex::new((None, StdRng::seed_from_u64(seed))),
        dist,
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
    seed: u64,
) -> DistributionGenerator<T, DiscreteDistribution<T>> {
    DistributionGenerator {
        state: Mutex::new((None, StdRng::seed_from_u64(seed))),
        dist: DiscreteDistribution::new(values),
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

    fn last(&self) -> Option<u64> {
        let val = self.counter.load(std::sync::atomic::Ordering::SeqCst);

        if val == 0 {
            None
        } else {
            Some(val - 1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_const_generator() {
        let gen = ConstGenerator::new(100);

        assert_eq!(gen.next(), 100);
        assert_eq!(gen.last(), Some(100));
    }

    #[test]
    fn test_counter_generator() {
        let gen = CounterGenerator::new(0);
        assert_eq!(gen.last(), None);

        for i in 0..10 {
            assert_eq!(gen.next(), i);
        }
        assert_eq!(gen.last(), Some(9));
    }
}
