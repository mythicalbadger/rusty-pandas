use rayon::iter::IntoParallelIterator;
use rand::seq::SliceRandom;
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

    /// Constructor for new Series
    pub fn new(data: Vec<f64>) -> Series {
        Series { data }
    }

    /// Returns the number of elements in the Series
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// True if the Series contains no elements, false otherwise
    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    /// Access a specific index inside the Series
    pub fn iloc(&self, idx: usize) -> f64 {
        *self.data.get(idx).expect("Not a valid index")
    }

    /// General use HOF for calling sequential/parallel depending on size of Series
    fn seq_or_par(&self, seq: &dyn Fn(&Self) -> Series, par: &dyn Fn(&Self) -> Series) -> Series {
        if self.size() < Series::LOWER_PAR_BOUND { seq(&self) }
        else { par(&self) }
    }

    /// Sums the values inside the Series
    pub fn sum(&self) -> Series {
        self.seq_or_par(&Series::seq_sum, &Series::par_sum)
    }

    /// Sums the values inside the Series sequentially
    fn seq_sum(&self) -> Series {
        Series::new(vec![self.data.iter().filter(|&&x| !x.is_nan()).sum()])
    }

    /// Sums the values inside the Series in parallel
    fn par_sum(&self) -> Series {
        Series::new(vec![(&self.data).into_par_iter().filter(|&&x| !x.is_nan()).sum()])
    }

    /// Returns a new series with all non-numerical/NaN values filtered out
    pub fn dropna(&self) -> Series {
        Series::new(self.data.clone().into_par_iter().filter(|&x| !x.is_nan()).collect())
    }

    /// Indicates indices with missing values
    pub fn isna(&self) -> Series {
        Series::new(self.data.clone().into_par_iter().map(|x| x.is_nan() as i32 as f64).collect())
    }

    /// Indicates existing (non-missing) values
    pub fn notna(&self) -> Series {
        Series::new(self.data.clone().into_par_iter().map(|x| !x.is_nan() as i32 as f64).collect())
    }

    /// Indicates whether or not the Series contains any elements that satisfy a predicate
    pub fn any(&self, pred: fn(f64) -> bool) -> bool {
        self.data.clone().into_par_iter().any(pred)
    }

    /// Calculates the mean of the values inside the Series
    pub fn mean(&self) -> Series {
        Series::new(vec![self.sum().iloc(0) / self.size() as f64])
    }

    /// Sorts the series
    pub fn sort(&self) -> Series {
        let mut sorted: Vec<f64> = self.dropna().data;
        sorted.par_sort_by(|a, b| a.partial_cmp(b).unwrap());
        Series::new(sorted)
    }

    /// Calculates the median of the values inside the Series
    pub fn median(&self) -> Series {
        let valid = self.dropna();
        if valid.is_empty() { return Series::zero() }
        if valid.size() == 1 { return Series::new(self.data.clone()) }

        let sorted = valid.sort();
        if valid.size() % 2 == 1 {
            let median = sorted.iloc(valid.size() / 2 as usize);
            Series::new(vec![median])
        }
        else {
            let median = (sorted.iloc(valid.size() / 2 - 1 as usize) + sorted.iloc(valid.size() / 2 as usize)) * 0.5;
            Series::new(vec![median])
        }
    }

    /// Calculates the minimum of the values inside the Series
    pub fn min(&self) -> Series {
        if self.is_empty() { Series::zero() }
        else {
            let dropna = self.dropna();
            let m = (&dropna.data)
                .into_par_iter()
                .reduce(|| &0.0, |x, y| if x < y {x} else {y});
            Series::new(vec![*m])
        }
    }

    /// Calculates the maximum of the values inside the Series
    pub fn max(&self) -> Series {
        if self.is_empty() { Series::zero() }
        else {
            let dropna = self.dropna();
            let m = (&dropna.data)
                .into_par_iter()
                .reduce(|| &0.0, |x, y| if x > y {x} else {y});
            Series::new(vec![*m])
        }
    }

}
