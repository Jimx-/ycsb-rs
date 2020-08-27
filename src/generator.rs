use crate::{Error, Result};

use std::{
    marker::PhantomData,
    sync::{atomic::AtomicU64, Arc},
};

use rand::{
    distributions::{Distribution, Uniform},
    Rng,
};

const ZIPFIAN_CONSTANT: f64 = 0.99;

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

pub struct ZipfDistribution {
    base: usize,
    num_items: usize,
    theta: f64,
    zeta_n: f64,
    alpha: f64,
    eta: f64,
}

impl ZipfDistribution {
    pub fn new(min: usize, max: usize, theta: f64) -> Result<Self> {
        if max < min {
            return Err(Error::InvalidArgument("max < min".to_owned()));
        }

        let num_items = max - min + 1;
        if num_items < 2 {
            return Err(Error::InvalidArgument("max - min < 2".to_owned()));
        }

        if theta == 1.0 {
            return Err(Error::InvalidArgument("theta == 1.0".to_owned()));
        }

        let alpha = 1.0 / (1.0 - theta);
        let zeta_2 = Self::zeta(2, theta);
        let zeta_n = Self::zeta(num_items, theta);

        let eta = (1.0 - (2.0 / num_items as f64).powf(1.0 - theta)) / (1.0 - zeta_2 / zeta_n);

        Ok(Self {
            base: min,
            num_items,
            theta,
            zeta_n,
            alpha,
            eta,
        })
    }

    fn zeta(num: usize, theta: f64) -> f64 {
        (0..num).map(|i| 1.0 / ((i + 1) as f64).powf(theta)).sum()
    }
}

impl Distribution<usize> for ZipfDistribution {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> usize {
        let u: f64 = rng.gen();
        let uz = u * self.zeta_n;

        if uz < 1.0 {
            return self.base;
        }

        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return self.base + 1;
        }

        self.base
            + (self.num_items as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as usize
    }
}

pub fn zipfian_gen(
    num_elements: usize,
    theta: f64,
) -> Result<DistributionGenerator<usize, ZipfDistribution>> {
    let dist = ZipfDistribution::new(0, num_elements, theta)?;

    Ok(DistributionGenerator {
        dist,
        value_type: PhantomData,
    })
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

    pub fn last_value(&self) -> u64 {
        self.counter.load(std::sync::atomic::Ordering::Relaxed)
    }
}
impl Generator<u64> for CounterGenerator {
    fn next(&self) -> u64 {
        self.counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

pub struct SkewedLatestGenerator {
    basis: Arc<CounterGenerator>,
    zipfian: ZipfDistribution,
}

impl SkewedLatestGenerator {
    pub fn new(basis: Arc<CounterGenerator>) -> Self {
        let max = basis.last_value();

        Self {
            basis,
            zipfian: ZipfDistribution::new(0, max as usize, ZIPFIAN_CONSTANT).unwrap(),
        }
    }
}

impl Generator<usize> for SkewedLatestGenerator {
    fn next(&self) -> usize {
        let max = self.basis.last_value();

        max as usize - self.zipfian.sample(&mut rand::thread_rng())
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
