use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use rayon::prelude::*;
use num_traits::*;
use std::ops::*;
use pyo3::prelude::*;

#[derive(Debug, Clone)]
#[pyclass]
pub struct Series {
    data: Vec<f64> 
}

#[pymethods]
impl Series {
    const LOWER_PAR_BOUND: usize = 8192;

    /// Creates a new Series
    ///
    /// # Example
    /// ```
    /// let sample_data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    /// let shiny_new_series: Series = Series::new(sample_data);
    /// ```
    #[new]
    pub fn new(data: Vec<f64>) -> Series {
        Series { data }
    }

    /// Returns the number of elements in the Series
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    /// let series: Series = Series::new(data);
    /// assert_eq!(series.size(), 5usize);
    /// ```
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// True if the Series contains no elements, false otherwise
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    /// let series: Series = Series::new(data);
    /// assert!(series.is_empty() == false);
    /// ```
    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    /// Access a specific index inside the Series
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    /// let series: Series = Series::new(data);
    /// assert_eq!(data.iloc(2), 2.0);
    /// ```
    pub fn iloc(&self, idx: usize) -> f64 {
        *self.data.get(idx).expect("Not a valid index")
    }

    /// Sums the values inside the Series
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    /// let series: Series = Series::new(data);
    /// assert_eq!(data.sum(), 15.0);
    /// ```
    pub fn sum(&self) -> Series {
        if self.size() < Series::LOWER_PAR_BOUND {
            Series::new(vec![(&self.dropna().data).iter().sum()])
        }
        else {
            Series::new(vec![(&self.dropna().data).par_iter().sum()])
        }
    }

    /// Computes the product of all values inside the Series
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    /// let series: Series = Series::new(data);
    /// assert_eq!(data.sum(), 120.0);
    /// ```
    pub fn prod(&self) -> Series {
        if self.size() < Series::LOWER_PAR_BOUND {
            Series::new(vec![(&self.dropna().data).iter().product()])
        }
        else {
            Series::new(vec![(&self.dropna().data).par_iter().product()])
        }
    }

    /// Returns a new Series with all non-numerical/NaN values filtered out
    ///
    /// # Example
    /// ```
    /// use std::f64::NAN;
    /// let data: Vec<f64> = vec![1.0, 2.0, NAN, 4.0, NAN];
    /// let data_expected: Vec<f64> = vec![1.0, 2.0, 4.0];
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(data_expected);
    /// assert_eq!(data.dropna(), expected);
    /// ```
    pub fn dropna(&self) -> Series {
        if self.size() < Series::LOWER_PAR_BOUND {
            Series::new(self.data.clone().into_iter().filter(|x| !x.is_nan()).collect())
        }
        else {
            Series::new(self.data.clone().into_par_iter().filter(|x| !x.is_nan()).collect())
        }
    }

    /// Indicates indices with missing values
    ///
    /// # Example
    /// ```
    /// use std::f64::NAN;
    /// let data: Vec<f64> = vec![1.0, 2.0, NAN, 4.0, NAN];
    /// let data_expected: Vec<f64> = vec![0.0, 0.0, 1.0, 0.0, 1.0];
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(data_expected);
    /// assert_eq!(data.isna(), expected);
    /// ```
    pub fn isna(&self) -> Series {
        if self.size() < Series::LOWER_PAR_BOUND {
            Series::new(self.data.clone().into_iter().map(|x| x.is_nan() as i32 as f64).collect())
        }
        else {
            Series::new(self.data.clone().into_par_iter().map(|x| x.is_nan() as i32 as f64).collect())
        }
    }

    /// Indicates existing (non-missing) values
    ///
    /// # Example
    /// ```
    /// use std::f64::NAN;
    /// let data: Vec<f64> = vec![1.0, 2.0, NAN, 4.0, NAN];
    /// let data_expected: Vec<f64> = vec![1.0, 1.0, 0.0, 1.0, 0.0];
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(data_expected);
    /// assert_eq!(data.notna(), expected);
    /// ```
    pub fn notna(&self) -> Series {
        if self.size() < Series::LOWER_PAR_BOUND {
            Series::new(self.data.clone().into_iter().map(|x| !x.is_nan() as i32 as f64).collect())
        }
        else {
            Series::new(self.data.clone().into_par_iter().map(|x| !x.is_nan() as i32 as f64).collect())
        }
    }

    /// Indicates whether or not the Series contains any elements that satisfy a predicate
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 3.0, 7.0, 14.0, 19.0];
    ///
    /// let series: Series = Series::new(data);
    /// let is_even = |x: f64| -> { x % 2 == 0 };
    /// assert!(data.any(is_even));
    /// ```
    pub fn any(&self, pred: &dyn Fn(f64) -> bool) -> bool {
        self.data.clone().into_par_iter().any(pred)
    }

    /// Sorts the series
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![3.0, 1.0, 4.0, 1.0, 5.0, 9.0];
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(vec![1.0, 1.0, 3.0, 4.0, 5.0, 9.0]);
    /// assert_eq!(series.sort(), expected);
    /// ```
    pub fn sort(&self) -> Series {
        let mut sorted: Vec<f64> = self.dropna().data;
        sorted.par_sort_by(|a, b| a.partial_cmp(b).unwrap());
        Series::new(sorted)
    }

    /// Calculates the mean of the values inside the Series
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 3.0, 7.0, 14.0, 19.0];
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(vec![8.8]);
    /// assert_eq!(series.mean(), expected);
    /// ```
    pub fn mean(&self) -> Series {
        if self.is_empty() { return Series::zero() }
        Series::new(vec![self.sum().iloc(0) / self.size() as f64])
    }

    /// Calculates the median of the values inside the Series
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 3.0, 7.0, 14.0, 19.0];
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(vec![7.0]);
    /// assert_eq!(series.median(), expected);
    /// ```
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

    /// Calculates the mode of values inside the Series
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 3.0, 3.0, 7.0, 14.0, 14.0 19.0, 19.0, 19.0];
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(vec![19.0]);
    /// assert_eq!(series.mode(), expected);
    /// ```
    pub fn mode(&self) -> Series {
        let valid = self.dropna();
        if valid.is_empty() { return Series::zero() }
        if valid.size() == 1 { return Series::new(self.data.clone()) }

        // We don't have groupBy identity so going to have to go a bit gonzo
        // Can't do HashMap/HashSet cause floats can't be hashed T_T
        let mut indices = vec![];
        let data = valid.sort();
        for i in 1..data.size() {
            if data.iloc(i-1) != data.iloc(i) { indices.push(i) }
        }
        let mut groups = vec![];
        groups.push(&data.data[0..indices[0]]);
        let mut chunks = indices.par_windows(2).map(|chunk| &data.data[chunk[0]..chunk[1]]).collect();
        groups.append(&mut chunks);
        groups.push(&data.data[indices[indices.len()-1]..data.data.len()]);

        Series::new(groups.into_par_iter().max_by_key(|g| g.len()).unwrap().to_vec())
    }

    /// Calculates the variance of values inside the Series
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0]
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(vec![2.5]);
    /// assert_eq!(series.var(), expected);
    /// ```
    pub fn var(&self) -> Series {
        if self.size() < Series::LOWER_PAR_BOUND {
            let valid = self.dropna();
            if valid.is_empty() { return Series::zero() }

            let n = valid.size() as f64;
            let mean = valid.mean().iloc(0);
            let variance = valid.data.into_iter().map(|x| pow(x-mean, 2)).sum::<f64>() / (n-1.0);

            Series::new(vec![variance])
        }
        else {
            let valid = self.dropna();
            if valid.is_empty() { return Series::zero() }

            let n = valid.size() as f64;
            let mean = valid.mean().iloc(0);
            let variance = valid.data.into_par_iter().map(|x| pow(x-mean, 2)).sum::<f64>() / (n-1.0);

            Series::new(vec![variance])
        }
    }

    /// Calculates the standard deviation of values inside the Series
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0]
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(vec![1.58]);
    /// assert_eq!(series.std(), expected);
    /// ```
    pub fn std(&self) -> Series {
        let variance = self.var();
        if variance.is_empty() { Series::zero(); }
        Series::new(vec![variance.iloc(0).sqrt()])
    }

    /// Calculates the minimum of the values inside the Series
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0]
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(vec![1.0]);
    /// assert_eq!(series.min(), expected);
    /// ```
    pub fn min(&self) -> Series {
        if self.is_empty() { Series::zero(); }

        if self.size() < Series::LOWER_PAR_BOUND {
            let dropna = self.dropna();
            let m = (&dropna.data)
                .into_iter()
                .reduce(|x, y| if x < y {x} else {y})
                .unwrap();
            Series::new(vec![*m])
        }
        else {
            let dropna = self.dropna();
            let m = (&dropna.data)
                .into_par_iter()
                .reduce(|| &0.0, |x, y| if x < y {x} else {y});
            Series::new(vec![*m])
        }
    }

    /// Calculates the maximum of the values inside the Series
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0]
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(vec![5.0]);
    /// assert_eq!(series.max(), expected);
    /// ```
    pub fn max(&self) -> Series {
        if self.is_empty() { Series::zero(); }

        if self.size() < Series::LOWER_PAR_BOUND {
            let dropna = self.dropna();
            let m = (&dropna.data)
                .into_iter()
                .reduce(|x, y| if x > y {x} else {y})
                .unwrap();
            Series::new(vec![*m])
        }
        else {
            let dropna = self.dropna();
            let m = (&dropna.data)
                .into_par_iter()
                .reduce(|| &0.0, |x, y| if x > y {x} else {y});
            Series::new(vec![*m])
        }
    }

    /*
    /// Applies a function to all elements and returns a new Series
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0]
    ///
    /// let mult2 = |x: f64| -> f64 { x * 2.0 };
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(vec![2.0, 4.0, 6.0, 8.0, 1.0]);
    /// assert_eq!(series.apply(mult2), expected);
    /// ```
    pub fn apply(&self, f: fn(f64) -> f64) -> Series {
        let applied = (&self.data).into_par_iter().map(|x| f(*x)).collect();
        Series::new(applied)
    }
    */

    /// Element wise addition
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0]
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(vec![6.0, 7.0, 8.0, 9.0, 10.0]);
    /// assert_eq!(series.plus(5), expected);
    /// ```
    pub fn plus(&self, n: f64) -> Series {
        Series::new((&self.data).into_par_iter().map(|x| x + n).collect())
    }

    /// Element wise subtraction
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0]
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(vec![-1.0, 0.0, 1.0, 2.0, 3.0]);
    /// assert_eq!(series.sub(2), expected);
    /// ```
    pub fn sub(&self, n: f64) -> Series {
        Series::new((&self.data).into_par_iter().map(|x| x - n).collect())
    }

    /// Element wise multiplication
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0]
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(vec![5.0, 10.0, 15.0, 20.0, 25.0, 30.0]);
    /// assert_eq!(series.mult(5), expected);
    /// ```
    pub fn mult(&self, n: f64) -> Series {
        Series::new((&self.data).into_par_iter().map(|x| x * n).collect())
    }

    /// Element wise division
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0]
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(vec![0.5, 1.0, 1.5, 2.0, 2.5]);
    /// assert_eq!(series.div(2), expected);
    /// ```
    pub fn div(&self, n: f64) -> Series {
        Series::new((&self.data).into_par_iter().map(|x| x / n).collect())
    }

    /// Calculates the cumulative/prefix sum of a Series
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0]
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(vec![1.0, 3.0, 6.0, 10.0, 15.0]);
    /// assert_eq!(series.cumsum(), expected);
    /// ```
    pub fn cumsum(&self) -> Series {
        // This looks awfully familiar
        fn prefix_sum(xs: &Vec<f64>) -> (Vec<f64>, f64) {    
            if xs.is_empty() { return (vec![], 0.0); }    

            // Speeds it up quite a bit    
            if xs.len() < 512 {    
                let mut pfs: Vec<f64> = vec![0.0];
                for i in 0..xs.len() {
                    pfs.push(xs[0..i+1].iter().sum());    
                }    
                return (pfs[0..pfs.len()-1].to_vec(), pfs[pfs.len()-1])    
            }    

            let half = xs.len() / 2;
            let (c_prefix, mut c_sum) = prefix_sum(
                &(0..half).into_par_iter()
                .map(|i| xs[i*2] + xs[i*2+1]) 
                .collect::<Vec<f64>>()    
              );    

            let mut pfs: Vec<f64> = (0..half).into_par_iter() 
                .flat_map(|i| vec![c_prefix[i], c_prefix[i]+xs[2*i]]) 
                .collect();    

            if xs.len() % 2 == 1 { pfs.push(c_sum); c_sum += xs[xs.len() - 1]; }    

            (pfs, c_sum)    
        }

        let (mut pfs, c_sum) = prefix_sum(&self.data);
        pfs.drain(0..1);
        pfs.push(c_sum);
        Series::new(pfs)
    }

    /// Joins the Series into string
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0]
    ///
    /// let series: Series = Series::new(data);
    /// assert_eq!(series.join(", "), "1, 2, 3, 4, 5".to_string());
    /// ```
    pub fn join(&self, token: &str) -> String {
        let joined: String = (&self.data).into_par_iter().map(|x| {
            if x.is_nan() { "NaN".to_string() + token}
            else { x.to_string() + token }
        }).collect();

        joined[0..joined.len() - token.len()].to_string()
    }

    /// Extracts a slice from the series
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0]
    ///
    /// let series: Series = Series::new(data);
    /// let expected: Series = Series::new(vec![3.0, 4.0]);
    /// assert_eq(series.slice(2, 4), expected);
    /// ```
    pub fn slice(&self, start: usize, end: usize) -> Series {
        let start = std::cmp::max(start, 0);
        let end = std::cmp::min(end, self.size());
        let slice = self.data[start..end].to_vec();
        Series::new(slice)
    }


    /// Computes the dot product of the Series and another
    ///
    /// # Example
    /// ```
    /// let a = Series::new(vec![1.0, 2.0, 3.0]);
    /// let b = Series::new(vec![4.0, -5.0, 6.0]);
    /// assert_eq!(a.dot(b).iloc(0), 12.0);
    /// ```
    pub fn dot(&self, other: Series) -> Series {
        if self.size() != other.size() { panic!("Series must have same dimensions"); }
        Series::new(
            vec![
                self.data.par_iter()
                    .zip(other.data.par_iter())
                    .map(|(&a, &b)| a * b)
                    .sum()
            ]
        )
    }

    /// Computes the vector sum of the Series and another
    ///
    /// # Example
    /// ```
    /// let a = Series::new(vec![1.0, 2.0, 3.0]);
    /// let b = Series::new(vec![4.0, -5.0, 6.0]);
    /// assert_eq!(a.vadd(b), Series::new(vec![5.0, -3.0, 9.0]));
    /// ```
    pub fn vadd(&self, other: Series) -> Series {
        if self.size() != other.size() { panic!("Series must have same dimensions"); }
        Series::new(
            self.data.par_iter()
                .zip(other.data.par_iter())
                .map(|(&a, &b)| a + b)
                .collect()
        )
    }

    /// Computes vector subtraction of the Series and another
    ///
    /// # Example
    /// ```
    /// let a = Series::new(vec![1.0, 2.0, 3.0]);
    /// let b = Series::new(vec![4.0, -5.0, 6.0]);
    /// assert_eq!(a.vsub(b), Series::new(vec![-3.0, 7.0, -3.0]));
    /// ```
    pub fn vsub(&self, other: Series) -> Series {
        if self.size() != other.size() { panic!("Series must have same dimensions"); }
        Series::new(
            self.data.par_iter()
                .zip(other.data.par_iter())
                .map(|(&a, &b)| a - b)
                .collect()
        )
    }

    /// Computes the norm/magnitude of the Series
    ///
    /// # Example
    /// ```
    /// let a = Series::new(vec![2.0, -2.0, 3.0, -4.0]);
    /// assert_eq!(a.norm().iloc(0), 33f64.sqrt());
    /// ```
    pub fn norm(&self) -> Series {
        Series::new(
            vec![
                self.data.par_iter()
                    .map(|&x| pow(x, 2))
                    .sum::<f64>()
                    .sqrt()
            ]
        )
    }

    /// Converts the Series to a Vector of f64
    ///
    /// # Example
    /// ```
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0]
    ///
    /// let series: Series = Series::new(data);
    /// assert_eq(series.to_vec(), data);
    /// ```
    pub fn to_vec(&self) -> Vec<f64> {
        self.data.to_vec()
    }

    fn __str__(&self) -> &'static str {
        Box::leak(format!("[{}]", self.join(", ")).into_boxed_str())
    }
    fn __repr__(&self) -> &'static str {
        Box::leak(format!("[{}]", self.join(", ")).into_boxed_str())
    }
}

macro_rules! from_num_type {
    ($type:ty) => {
        impl From<$type> for Series {
            fn from(val: $type) -> Self {
                Self { data: vec![val as f64] }
            }
        }
    }
}

macro_rules! from_vec_type {
    ($type:ty) => {
        impl From<Vec<$type>> for Series {
            fn from(val: Vec<$type>) -> Self {
                Series { data: val.iter().map(|&x| x as f64).collect() }
            }
        }
    }
}

macro_rules! from_vec_ref_type {
    ($type:ty) => {
        impl From<&Vec<$type>> for Series {
            fn from(val: &Vec<$type>) -> Self {
                Series { data: val.iter().map(|&x| x as f64).collect() }
            }
        }
    }
}

macro_rules! from_range_type {
    ($type:ty) => {
        impl From<Range<$type>> for Series {
            fn from(val: Range<$type>) -> Self {
                Self { data: val.map(|x| x as f64).collect() }
            }
        }
    }
}

macro_rules! from_range_incl_type {
    ($type:ty) => {
        impl From<RangeInclusive<$type>> for Series {
            fn from(val: RangeInclusive<$type>) -> Self {
                Self { data: val.map(|x| x as f64).collect() }
            }
        }
    }
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

impl PartialEq for Series {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for Series {}

from_num_type!(f64);
from_num_type!(f32);
from_num_type!(i8);
from_num_type!(i16);
from_num_type!(i32);
from_num_type!(i64);
from_num_type!(u8);
from_num_type!(u16);
from_num_type!(u32);
from_num_type!(u64);

from_vec_type!(f64);
from_vec_type!(f32);
from_vec_type!(i8);
from_vec_type!(i16);
from_vec_type!(i32);
from_vec_type!(i64);
from_vec_type!(u8);
from_vec_type!(u16);
from_vec_type!(u32);
from_vec_type!(u64);

from_vec_ref_type!(f64);
from_vec_ref_type!(f32);
from_vec_ref_type!(i8);
from_vec_ref_type!(i16);
from_vec_ref_type!(i32);
from_vec_ref_type!(i64);
from_vec_ref_type!(u8);
from_vec_ref_type!(u16);
from_vec_ref_type!(u32);
from_vec_ref_type!(u64);

from_range_type!(i8);
from_range_type!(i16);
from_range_type!(i32);
from_range_type!(i64);
from_range_type!(u8);
from_range_type!(u16);
from_range_type!(u32);
from_range_type!(u64);

from_range_incl_type!(i8);
from_range_incl_type!(i16);
from_range_incl_type!(i32);
from_range_incl_type!(i64);
from_range_incl_type!(u8);
from_range_incl_type!(u16);
from_range_incl_type!(u32);
from_range_incl_type!(u64);
