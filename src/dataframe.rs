#![allow(dead_code)]
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use rayon::prelude::*;
use std::fs;
use crate::series::*;
use num_traits::Zero;
use std::ops::Index;
use std::fmt::{Display, Formatter, Result};
use glob::glob;

/*
 *
 * To IMPLEMENT
 * - size (done)
 * - sum (done)
 * - mean (done)
 * - min (done)
 * - max ( done)
 * - apply 
 * - copy (done)
 * - count
 * - cumsum
 * - describe
 * - dot
 * - divide
 * - dropna
 * - equals
 * - from_dict
 * - head
 * - tail
 * - insert
 * - dropna (done)
 * - median (done)
 * - memory usage (cool)
 * - mode 
 * - read_csv (done)
 * - read_excel
 * - to_csv (done)
 * - prod
 * - to_dict
 * - transpose 
 * - read_csv from folder (done)
 * - read_csv from glob
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

    /// Calculates the minimum of values inside the DataFrame
    pub fn min(&self, axis: usize) -> DataFrame {
        let (df, header) = self.parse_axis(axis);
        DataFrame::new( df.par_iter().map(|s| s.min()).collect(), header )
    }

    /// Calculates the minimum of values inside the DataFrame
    pub fn max(&self, axis: usize) -> DataFrame {
        let (df, header) = self.parse_axis(axis);
        DataFrame::new( df.par_iter().map(|s| s.max()).collect(), header )
    }

    /// Creates a deepcopy of a DataFrame
    pub fn copy(&self) -> DataFrame {
        let data_copy = self.cols.clone().into_par_iter().map(|col| col.clone()).collect();
        let header_copy = self.header_row.clone();
        DataFrame::new(data_copy, Some(header_copy))
    }

    pub fn to_csv(&self, filename: &str) {
        let header: String = self.header_row.join(",") + "\n";
        let out: Vec<String> = (&self.rows).into_par_iter().map(|r| r.join(",")).collect();
        fs::write(filename, header + &out.join("\n")).expect("Unable to write to file");
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
