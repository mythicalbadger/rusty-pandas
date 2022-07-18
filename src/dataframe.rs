#![allow(dead_code)]
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use rayon::prelude::*;
use std::fs;
use crate::series::*;
use num_traits::Zero;
use std::ops::{Index, Add, Sub, Div, Mul};
use std::fmt::{Display, Formatter, Result};
use glob::glob;
use std::collections::HashMap;

/*
 *
 * To IMPLEMENT
 * - size (done)
 * - sum (done)
 * - mean (done)
 * - min (done)
 * - max ( done)
 * - variance (done)
 * - standard deviation (done)
 * - apply (done)
 * - copy (done)
 * - count
 * - cumsum
 * - describe
 * - dot
 * - divide
 * - dropna (done)
 * - equals (done)
 * - from_dict
 * - head (done)
 * - tail (done)
 * - insert
 * - dropna (done)
 * - median (done)
 * - mode (done)
 * - read_csv (done)
 * - read_excel
 * - to_csv (done)
 * - prod (done)
 * - to_dict (done)
 * - from_dict (done)
 * - transpose 
 * - read_csv from folder (done)
 * - read_csv from glob (done)
 *
 */

#[derive(Debug)]
pub struct DataFrame {
    header_row: Vec<String>, 
    cols: Vec<Series>,
    rows: Vec<Series>,
    pub size: usize
}

impl Display for DataFrame {
    fn fmt(&self, f: &mut Formatter) -> Result {
        let out: Vec<String> = self.header_row.iter().zip(&self.cols).map(|(h, d)| format!("{h}: {d}")).collect();
        write!(f, "{:?}", out.join(", "))
    }
}

impl PartialEq for DataFrame {
    fn eq(&self, other: &Self) -> bool {
        if self.size() != other.size() { return false; }

        self.header_row == other.header_row &&
        self.cols == other.cols
    }
}

impl Eq for DataFrame {}

impl Index<usize> for DataFrame {
    type Output = Series;

    fn index(&self, idx: usize) -> &Self::Output {
        &self.rows[idx]
    }
}

impl DataFrame {
    const LOWER_PAR_BOUND: usize = 8192;
    
    /// Generates the default header row
    fn gen_default_header(len: usize) -> Vec<String> {
        (0..len).into_par_iter().map(|x| x.to_string()).collect()
    }

    /// Returns reference to row/column and appropriate header depending on axis
    fn parse_axis(&self, axis: usize) -> (&Vec<Series>, Option<Vec<String>>) {
        if axis == 0 {
            (&self.cols, Some(self.header_row.clone()))
        }
        else {
            (&self.rows, None)
        }
    }

    /// Constructor for the DataFrame
    pub fn new(data: Vec<Series>, header_row: Option<Vec<String>>) -> DataFrame {
        let rows = transpose(&data);
        let size = rows.len() * data.len();
        DataFrame { 
            header_row : header_row.unwrap_or(DataFrame::gen_default_header(rows.get(0).unwrap_or(&Series::zero()).size())), 
            cols : data, 
            rows,
            size 
        }
    }
    
    /// Extract a row from the DataFrame
    pub fn irow(&self, row: usize) -> Series {
        self.rows[row].clone()
    }

    /// Extract a column from the DataFrame
    pub fn icol(&self, col: usize) -> Series {
        self.cols[col].clone()
    }

    /// Returns the length/size of DataFrame
    pub fn size(&self) -> usize {
        self.cols.len() as usize
    }

    /// Drops any rows/columns that contain missing values
    pub fn dropna(&self, axis: usize) -> DataFrame {
        let (df, header) = self.parse_axis(axis);
        let is_true = |x: f64| { x == 1.0};
        DataFrame::new(
            df.par_iter()
              .filter(|s| !s.isna().any(is_true))
              .map(|s| s.clone()).collect(),

              header
        )
    }

    /// Alias for dropna
    pub fn dropnull(&self, axis: usize) -> DataFrame {
        self.dropna(axis) // clickbaited
    }

    /// Sums dataframe - columns: 0, rows: 1
    pub fn sum(&self, axis: usize) -> DataFrame {
        let (df, header) = self.parse_axis(axis);
        DataFrame::new(df.par_iter().map(|s| s.sum()).collect(), header)
    }

    pub fn prod(&self, axis: usize) -> DataFrame {
        let (df, header) = self.parse_axis(axis);
        DataFrame::new(df.par_iter().map(|s| s.prod()).collect(), header)
    }

    /// Calculates the mean of values inside the DataFrame
    pub fn mean(&self, axis: usize) -> DataFrame {
        let (df, header) = self.parse_axis(axis);
        DataFrame::new( df.par_iter().map(|s| s.mean()).collect(), header )
    }

    /// Calculates the median of values inside the DataFrame
    pub fn median(&self, axis: usize) -> DataFrame {
        let (df, header) = self.parse_axis(axis);
        DataFrame::new( df.par_iter().map(|s| s.median()).collect(), header )
    }

    /// Calculates the mode of values inside the DataFrame
    pub fn mode(&self, axis: usize) -> DataFrame {
        let (df, header) = self.parse_axis(axis);
        DataFrame::new( df.par_iter().map(|s| s.mode()).collect(), header )
    }

    /// Calculates the variance of values inside the DataFrame
    pub fn var(&self, axis: usize) -> DataFrame {
        let valid = self.dropna(axis);
        let (df, header) = valid.parse_axis(axis);
        DataFrame::new( df.par_iter().map(|s| s.var()).collect(), header )
    }

    /// Calculates the standard deviation of values inside the DataFrame
    pub fn std(&self, axis: usize) -> DataFrame {
        let valid = self.dropna(axis);
        let (df, header) = valid.parse_axis(axis);
        DataFrame::new( df.par_iter().map(|s| s.std()).collect(), header )
    }

    /// Calculates the minimum of values inside the DataFrame
    pub fn min(&self, axis: usize) -> DataFrame {
        let (df, header) = self.parse_axis(axis);
        DataFrame::new( df.par_iter().map(|s| s.min()).collect(), header )
    }

    /// Calculates the maximum of values inside the DataFrame
    pub fn max(&self, axis: usize) -> DataFrame {
        let (df, header) = self.parse_axis(axis);
        DataFrame::new( df.par_iter().map(|s| s.max()).collect(), header )
    }

    /// Applies a function to all values
    pub fn apply(&self, f: fn(f64) -> f64) -> DataFrame {
        let header = Some(self.header_row.clone());
        let applied = (&self.cols).into_par_iter()
            .map(|x| x.apply(f))
            .collect();
        DataFrame::new(applied, header)
    }

    /// Creates a deepcopy of a DataFrame
    pub fn copy(&self) -> DataFrame {
        let data_copy = self.cols.clone().into_par_iter().map(|col| col.clone()).collect();
        let header_copy = self.header_row.clone();
        DataFrame::new(data_copy, Some(header_copy))
    }

    /// Writes the contents of the DataFrame to a CSV file
    pub fn to_csv(&self, filename: &str) {
        let header: String = self.header_row.join(",") + "\n";
        let out: Vec<String> = (&self.rows).into_par_iter().map(|r| r.join(",")).collect();
        fs::write(filename, header + &out.join("\n")).expect("Unable to write to file");
    }

    /// Converts the DataFrame to a HashMap
    pub fn to_hashmap(&self) -> HashMap<String, Vec<f64>> {
        let zipped: Vec<(String, Vec<f64>)> = self.header_row.clone().into_par_iter().zip(self.cols.clone().into_par_iter().map(|s| s.to_vec())).collect();
        HashMap::from_par_iter(zipped)
    }

    /// Extracts the first N rows of the DataFrame
    pub fn head(&self, n: usize) -> DataFrame {
        let sliced = (&self.cols).into_par_iter()
            .map(|x| {
                x.slice(0, n)
            })
            .collect();

        DataFrame::new(sliced, Some(self.header_row.clone()))
    }

    /// Extracts the last N rows of the DataFrame
    pub fn tail(&self, n: usize) -> DataFrame {
        let sliced = (&self.cols).into_par_iter()
            .map(|x| {
                x.slice(x.size() - n, x.size())
            })
            .collect();

        DataFrame::new(sliced, Some(self.header_row.clone()))
    }

    /// Adds a value to all elements in the DataFrame
    pub fn plus(&self, n: f64) -> DataFrame {
        // One would think this would be a good opportunity to *apply* our apply but since Rust
        // won't allow us to capture variables, we can't pass in a func that uses n...
        let header = Some(self.header_row.clone());
        let applied = (&self.cols).into_par_iter()
            .map(|x| x.plus(n))
            .collect();
        DataFrame::new(applied, header)
    }

    /// Subtracts a value to all elements in the DataFrame
    pub fn sub(&self, n: f64) -> DataFrame {
        let header = Some(self.header_row.clone());
        let applied = (&self.cols).into_par_iter()
            .map(|x| x.sub(n))
            .collect();
        DataFrame::new(applied, header)
    }

    /// Multiplies a value to all elements in the DataFrame
    pub fn mult(&self, n: f64) -> DataFrame {
        let header = Some(self.header_row.clone());
        let applied = (&self.cols).into_par_iter()
            .map(|x| x.mult(n))
            .collect();
        DataFrame::new(applied, header)
    }

    /// Divides a value to all elements in the DataFrame
    pub fn div(&self, n: f64) -> DataFrame {
        let header = Some(self.header_row.clone());
        let applied = (&self.cols).into_par_iter()
            .map(|x| x.div(n))
            .collect();
        DataFrame::new(applied, header)
    }

    /// Computes the cumulative/prefix sum over all rows/cols of the DataFrame
    pub fn cumsum(&self, axis: usize) -> DataFrame {
        let (df, header) = self.parse_axis(axis);
        DataFrame::new(df.into_par_iter().map(|s| s.cumsum()).collect(), header)
    }
}

pub fn transpose(mat: &Vec<Series>) -> Vec<Series> {
    if mat.len() == 0 { return mat.to_vec() }
    (0..mat[0].size()).into_par_iter()
        .map(|i| {
        Series::new( mat.par_iter()
                        .map(|c| c.iloc(i))
                        .collect() 
                   )    
    }).collect()
}

pub fn read_csv(filename: &str) -> DataFrame {
    let file = fs::read_to_string(filename).expect("Something went wrong when reading");
    let lines: Vec<&str> = file.trim().par_split('\n').collect();
    let header_row: Vec<String> = lines[0].par_split(',').map(|x| String::from(x)).collect();
    let data: Vec<Series> = lines[1..].par_iter().map(|&line| {
        Series::new(line.par_split(',').map(|elt| {
            match elt.parse::<f64>() {
                Ok(f) => f,
                Err(_) => f64::NAN
            }
        }).collect())
    }).collect();
    let df_data = transpose(&data);
    DataFrame::new(df_data, Some(header_row))
}

pub fn read_csv_from_folder(folder_name: &str) -> Vec<DataFrame> {
    let paths: Vec<std::path::PathBuf> = fs::read_dir(folder_name)
        .expect("Something went wrong")
        .into_iter()
        .filter(|x| x.is_ok())
        .map(|p| p.unwrap().path())
        .collect();

    paths.par_iter()
         .filter(|p| p.to_str().unwrap().ends_with(".csv"))
         .map(|p| read_csv(p.to_str().unwrap()))
         .collect()
}

pub fn read_csv_by_glob(path: &str, expr: &str) -> Vec<DataFrame> {
    let paths: Vec<std::path::PathBuf> = glob(format!("{}{}", path, expr).as_str()).expect("Failed to read pattern")
        .par_bridge()
        .filter(|p| p.is_ok())
        .map(|p| p.unwrap())
        .collect();

    paths.into_par_iter()
         .filter(|p| p.to_str().unwrap().ends_with(".csv"))
         .map(|p| read_csv(p.to_str().unwrap()))
         .collect()
}

pub fn from_hashmap(data_map: HashMap<String, Vec<f64>>) -> DataFrame {
    let header: Vec<String> = data_map.keys().map(|x| x.clone()).collect();
    let data: Vec<Series> = data_map.values().map(|x| Series::new(x.clone())).collect();
    DataFrame::new(data, Some(header))
}
