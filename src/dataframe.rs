#![allow(dead_code)]
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::ops::Deref;
use std::fs;

pub enum DataType {
    Str, Num, Bool, NaN
}

pub struct DataNum {
    data_type: DataType,
    value: f64
}

pub struct DataStr {
    data_type: DataType,
    value: String
}

pub struct DataBool {
    data_type: DataType,
    value: bool
}

pub struct DataFrame<> {
    data: Vec<f64>,
}

impl std::fmt::Display for DataFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.data)
    }
}

impl Deref for DataFrame {
    type Target = Vec<f64>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DataFrame {
    const LOWER_PAR_BOUND: i64 = 8192;

    /// Constructor for the DataFrame
    pub fn new(data: Vec<f64>) -> DataFrame {
        DataFrame { data }
    }

    /// General use HOF for calling sequential/parllel depending on size of DataFrame
    fn seq_or_par<T>(&self, seq: &dyn Fn(&Self) -> T, par: &dyn Fn(&Self) -> T) -> T {
        if self.size() < DataFrame::LOWER_PAR_BOUND { seq(&self) }
        else { par(&self) }
    }

    /// Returns the length/size of DataFrame
    pub fn size(&self) -> i64 {
        self.data.len() as i64
    }

    /// Sums the values inside the DataFrame
    pub fn sum(&self) -> f64 {
        self.seq_or_par(&DataFrame::seq_sum, &DataFrame::par_sum)
    }

    /// Sums the values inside the DataFrame sequentially
    pub fn seq_sum(&self) -> f64 {
        (&self.data).iter().sum()
    }

    /// Sums the values inside the DataFrame in parallel
    pub fn par_sum(&self) -> f64 {
        (&self.data).into_par_iter().sum()
    }

    /// Calculates the mean of values inside the DataFrame
    pub fn mean(&self) -> f64 {
        self.sum() / self.size() as f64
    }

    /// Returns a DataFrame with all positive/absolute values
    pub fn abs(&self) -> DataFrame {
        self.seq_or_par(&DataFrame::seq_abs, &DataFrame::par_abs)
    }

    /// Converts DataFrame values to absolute sequentially
    fn seq_abs(&self) -> DataFrame {
        DataFrame::new((&self.data).iter()
                    .map(|x| x.abs())
                    .collect())
    }

    /// Converts DataFrame values to absolute in parallel
    fn par_abs(&self) -> DataFrame {
        DataFrame::new((&self.data).into_par_iter()
                    .map(|x| x.abs())
                    .collect())
    }

    pub fn min(&self) -> f64 {
        self.seq_or_par(&DataFrame::seq_min, &DataFrame::par_min)
    }

    fn seq_min(&self) -> f64 {
        // Since floats are partially ordered, have to use custom comparator
        let m = (&self.data).iter()
                            .min_by(|&x, &y| x.partial_cmp(y).unwrap()); 
        match m {
            None => f64::MIN,
            Some(&i) => i
        }
    }

    fn par_min(&self) -> f64 {
        // Since floats are partially ordered, have to use custom comparator
        let m = (&self.data).into_par_iter()
                            .min_by(|&x, &y| x.partial_cmp(y).unwrap()); 
        match m {
            None => f64::MIN,
            Some(&i) => i
        }
    }
}

pub fn read_csv(filename: &str) -> DataFrame {
    let file = fs::read_to_string(filename).expect("Something went wrong when reading");
    let data: Vec<f64> = file.trim().split("\r\n").map(|x| {
        match x.parse::<f64>() {
            Ok(f) => f,
            Err(_) => f64::NAN
        }
    }).collect();
    DataFrame::new(data)
}
