use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use rayon::prelude::*;
use num_traits::*;
use std::iter::Sum;
use std::ops::*;

#[derive(Debug, Clone)]
pub struct Series {
    data: Vec<f64> 
}

impl std::fmt::Display for Series {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.data)
    }
}

impl Add for Series {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let mut data = self.data;
        data.extend(other.data);
        Self {
            data
        }
    }
}

impl Zero for Series {
    fn zero() -> Self { Self { data: vec![] } }
    fn is_zero(&self) -> bool { self.is_empty() }
}

impl Series {
    const LOWER_PAR_BOUND: usize = 8192;

    pub fn new(data: Vec<f64>) -> Series {
        Series { data }
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    pub fn iloc(&self, idx: usize) -> f64 {
        self.data[idx]
    }

    /// General use HOF for calling sequential/parllel depending on size of DataFrame
    fn seq_or_par(&self, seq: &dyn Fn(&Self) -> Series, par: &dyn Fn(&Self) -> Series) -> Series {
        if self.size() < Series::LOWER_PAR_BOUND { seq(&self) }
        else { par(&self) }
    }

    pub fn sum(&self) -> Series {
        self.seq_or_par(&Series::seq_sum, &Series::par_sum)
    }

    /// Sums the values inside the DataFrame sequentially
    fn seq_sum(&self) -> Series {
        Series::new(vec![self.data.iter().filter(|&&x| !x.is_nan()).sum()])
    }

    /// Sums the values inside the DataFrame in parallel
    fn par_sum(&self) -> Series {
        Series::new(vec![(&self.data).into_par_iter().filter(|&&x| !x.is_nan()).sum()])
    }

    pub fn mean(&self) -> Series {
        Series::new(vec![self.sum().iloc(0) / self.size() as f64])
    }

    pub fn min(&self) -> Series {
        if self.is_empty() { Series::zero() }
        else {
            let m = (&self.data).into_par_iter()
                .filter(|&&x| !x.is_nan())
                .reduce(|| &0.0, |x, y| if x < y {x} else {y});
            Series::new(vec![*m])
        }
    }

}
