use std::sync::Mutex;

use rand::{
    distributions::{Distribution, Uniform},
    rngs::StdRng,
    SeedableRng,
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
) -> Result<DistributionGenerator<usize, ZipfDistribution>, ()> {
    let dist = ZipfDistribution::new(num_elements, exponent)?;

    Ok(DistributionGenerator {
        state: Mutex::new((None, StdRng::seed_from_u64(seed))),
        dist,
    })
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
}
