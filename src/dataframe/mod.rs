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
use std::collections::HashMap;
use pyo3::prelude::*;
use prettytable::{Table, Row};

const LOWER_PAR_BOUND: usize = 8192;

#[derive(Debug)]
#[pyclass]
pub struct DataFrame {
    header_row: Vec<String>, 
    cols: Vec<Series>,
    rows: Vec<Series>,
    pub size: usize
}

macro_rules! parse_axis {
    ($self:ident, $method:ident, $axis: expr) => {
        if $axis == 0 { 
            if $self.cols.len() < LOWER_PAR_BOUND {
                DataFrame::new($self.cols.iter().map(|s| s.$method()).collect(), Some($self.header_row.clone())) 
            }
            else {
                DataFrame::new($self.cols.par_iter().map(|s| s.$method()).collect(), Some($self.header_row.clone())) 
            }
        }
        else { 
            if $self.rows.len() < LOWER_PAR_BOUND {
                DataFrame::new($self.rows.iter().map(|s| s.$method()).collect(), None) 
            }
            else {
                DataFrame::new($self.rows.par_iter().map(|s| s.$method()).collect(), None) 
            }
        }
    };
}

#[pymethods]
impl DataFrame {

    /// Creates a new DataFrame
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    ///
    /// ```
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    ///
    /// ```
    /// Create a new DataFrame of the form
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    ///
    /// ```
    ///
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, None);
    /// ```
    #[new]
    pub fn new(data: Vec<Series>, header_row: Option<Vec<String>>) -> DataFrame {
        let rows = transpose(&data);
        let size = rows.len() * data.len();
        let header = header_row.unwrap_or(
            DataFrame::gen_default_header(
                rows.get(0).unwrap_or(&Series::zero()).size()
            )
        );
        DataFrame { 
            header_row : header,
            cols : data, 
            rows,
            size 
        }
    }
    
    /// Extract a row from the DataFrame by index
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and access the second row
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// println!("{}", df.irow(1));
    /// ```
    pub fn irow(&self, row: usize) -> Series {
        self.rows[row].clone()
    }

    /// Extract a column from the DataFrame by index
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and access the first column
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// println!("{}", df.icol(0));
    /// ```
    pub fn icol(&self, col: usize) -> Series {
        self.cols[col].clone()
    }

    /// Extract a column from the DataFrame by name
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and access the Height column
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// println!("{}", df.loc_col("Height").unwrap());
    /// ```
    pub fn loc_col(&self, col_name: &str) -> Option<Series> {
        let idx = self.header_row.iter().position(|c| c == col_name);
        match idx {
            Some(i) => Some(self.icol(i)),
            _ => None
        }
    }

    /// Returns the length/size of DataFrame
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and access the find the size
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// assert_eq!(df.size(), 3usize);
    /// ```
    pub fn size(&self) -> usize {
        self.cols.len() as usize
    }

    /// Drops any rows/columns that contain missing values
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and drop all Series that contain missing values
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  NaN   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  NaN   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![f64::NAN, 160.0, f64::NAN])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    ///
    /// // Drop columns
    /// println!("{}", df.dropna(0));
    ///
    /// // Drop rows
    /// println!("{}", df.dropna(1));
    /// ```
    pub fn dropna(&self, axis: usize) -> DataFrame {
        let is_true = |x: f64| { x == 1.0};
        if axis == 0 {
            DataFrame::new(
                self.cols.par_iter()
                  .filter(|&s| !s.isna().to_vec().into_iter().any(is_true))
                  .map(|s| s.clone()).collect::<Vec<Series>>().clone(),
                  Some(self.header_row.clone())
            )
        }
        else {
            DataFrame::new(
                self.rows.par_iter()
                  .filter(|&s| !s.isna().to_vec().into_iter().any(is_true))
                  .map(|s| s.clone()).collect::<Vec<Series>>().clone(),
                  None
            )
        }
    }

    /// Alias for dropna
    pub fn dropnull(&self, axis: usize) -> DataFrame {
        self.dropna(axis) // clickbaited
    }

    /// Sums each Series in the DataFrame
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and sums all Series 
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// 
    /// // Sum across columns
    /// println!("{}", df.sum(0))
    /// 
    /// // Sum across rows
    /// println!("{}", df.sum(1))
    /// ```
    pub fn sum(&self, axis: usize) -> DataFrame {
        parse_axis!(self, sum, axis)
    }

    /// Computes the product over values for each Series in the DataFrame
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and compute the product of all Series 
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// 
    /// // Product across columns
    /// println!("{}", df.prod(0))
    /// 
    /// // Product across rows
    /// println!("{}", df.prod(1))
    /// ```
    pub fn prod(&self, axis: usize) -> DataFrame {
        parse_axis!(self, prod, axis)
    }

    /// Calculates the mean for each Series in the DataFrame
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and calculate the means
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// 
    /// // Mean across columns
    /// println!("{}", df.mean(0))
    /// 
    /// // Mean across rows
    /// println!("{}", df.mean(1))
    /// ```
    pub fn mean(&self, axis: usize) -> DataFrame {
        parse_axis!(self, mean, axis)
    }

    /// Calculates the median for each Series in the DataFrame
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and calculate the medians
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// 
    /// // Median across columns
    /// println!("{}", df.median(0))
    /// 
    /// // Median across rows
    /// println!("{}", df.median(1))
    /// ```
    pub fn median(&self, axis: usize) -> DataFrame {
        parse_axis!(self, median, axis)
    }

    /// Calculates the mode for each Series in the DataFrame
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and calculate the mode
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// 
    /// // Mode across columns
    /// println!("{}", df.mode(0))
    /// 
    /// // Mode across rows
    /// println!("{}", df.mode(1))
    /// ```
    pub fn mode(&self, axis: usize) -> DataFrame {
        parse_axis!(self, mode, axis)
    }

    /// Calculates the variance for each Series in the DataFrame
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and calculate the variance
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// 
    /// // Variance across columns
    /// println!("{}", df.var(0))
    /// 
    /// // Variance across rows
    /// println!("{}", df.var(1))
    /// ```
    pub fn var(&self, axis: usize) -> DataFrame {
        let valid = self.dropna(axis);
        parse_axis!(valid, var, axis)
    }

    /// Calculates the standard deviation for each Series in the DataFrame
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and calculate the standard deviation
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// 
    /// // Standard deviation across columns
    /// println!("{}", df.std(0))
    /// 
    /// // Standard deviation across rows
    /// println!("{}", df.std(1))
    /// ```
    pub fn std(&self, axis: usize) -> DataFrame {
        let valid = self.dropna(axis);
        parse_axis!(valid, std, axis)
    }

    /// Calculates the minimum for each Series in the DataFrame
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and calculate minimum
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// 
    /// // Minimum across columns
    /// println!("{}", df.min(0))
    /// 
    /// // Minimum across rows
    /// println!("{}", df.min(1))
    /// ```
    pub fn min(&self, axis: usize) -> DataFrame {
        parse_axis!(self, min, axis)
    }

    /// Calculates the maximum for each Series in the DataFrame
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and calculate maximum
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// 
    /// // Maximum across columns
    /// println!("{}", df.max(0))
    /// 
    /// // Maximum across rows
    /// println!("{}", df.max(1))
    /// ```
    pub fn max(&self, axis: usize) -> DataFrame {
        parse_axis!(self, max, axis)
    }
   /* 

    /// Applies a function to all values inside the DataFrame
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and divide all by 10
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// let f = |x: f64| -> f64 { x / 10.0 };
    /// df.apply(f);
    /// ```
    pub fn apply(&self, f: fn(f64) -> f64) -> DataFrame {
        let header = Some(self.header_row.clone());
        let applied = (&self.cols).into_par_iter()
            .map(|x| Series::new(x.to_vec().into_par_iter().map(|x| f(x)).collect()))
            .collect();
        DataFrame::new(applied, header)
    }
    */

    /// Creates a deepcopy of a DataFrame
    pub fn copy(&self) -> DataFrame {
        let data_copy = self.cols.clone().into_par_iter().map(|col| col.clone()).collect();
        let header_copy = self.header_row.clone();
        DataFrame::new(data_copy, Some(header_copy))
    }

    /// Writes the contents of the DataFrame to a CSV file
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and write it to a CSV file
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// let path: &str = "/tmp/wtfbbq.csv";
    /// df.to_csv(path);
    /// ```
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
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and extract the first two rows
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// println!("{}", df.head(2));
    /// ```
    pub fn head(&self, n: usize) -> DataFrame {
        let sliced = (&self.cols).into_par_iter()
            .map(|x| {
                x.slice(0, n)
            })
            .collect();

        DataFrame::new(sliced, Some(self.header_row.clone()))
    }

    /// Extracts the last N rows of the DataFrame
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and extract the last two rows
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// println!("{}", df.head(2));
    /// ```
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
        let applied = (&self.cols).iter()
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

    /// Computes the cumulative/prefix sum for each Series in the DataFrame
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and compute the cumulative sum
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    ///
    /// // Over columns
    /// println!("{}", df.cumsum(0));
    ///
    /// // Over rows
    /// println!("{}", df.cumsum(1));
    /// ```
    pub fn cumsum(&self, axis: usize) -> DataFrame {
        parse_axis!(self, cumsum, axis)
    }

    /// Returns a new DataFrame with a new column inserted into it 
    ///
    /// # Examples
    ///
    /// Create a new DataFrame of the form and adds a column for Weight
    /// | UserID |  Age  | Height |
    /// |   0    |   42  |  183   |
    /// |   1    |   21  |  160   |
    /// |   2    |   8   |  132   |
    /// ```
    ///
    /// let header: Vec<String> = vec!["UserID".to_string(), "Age".to_string(), "Height".to_string()];
    /// let data: Vec<Series> = vec![
    ///     Series::new(vec![0.0, 1.0, 2.0]),
    ///     Series::new(vec![42.0, 21.0, 8.0]),
    ///     Series::new(vec![183.0, 160.0, 132.0])
    /// ];
    ///
    /// let weight_data: Series = Series::new(vec![100.0, 300.0, 77.0]);
    /// let df: DataFrame = DataFrame::new(data, Some(header));
    /// let df = df.insert_col(3, "Weight" weight_data);
    /// println!("{}", df);
    /// ```
    pub fn insert_col(&self, pos: usize, column_name: &str, column: Series) -> DataFrame {
        // Suckier than normal insertion since we are creating an entirely new DataFrame
        if pos > self.cols.len() + 1 { panic!("Invalid index"); }
        let mut cols = self.cols.clone();
        let mut headers = self.header_row.clone();
        cols.insert(pos, column);
        headers.insert(pos, column_name.to_string());
        let size = cols.len() * self.rows.len();

        DataFrame {
            header_row: headers,
            cols,
            rows: self.rows.clone(),
            size
        }
    }
    
    /// Generates the default header row
    #[staticmethod]
    fn gen_default_header(len: usize) -> Vec<String> {
        (0..len).into_par_iter().map(|x| x.to_string()).collect()
    }

    fn __str__(&self) -> &'static str {
        let out: String = self.header_row.iter().zip(&self.cols).map(|(h, d)| format!("{h}: {d}")).collect::<Vec<String>>().join(", ");
        Box::leak(out.into_boxed_str())
    }
    fn __repr__(&self) -> &'static str {
        let out: String = self.header_row.iter().zip(&self.cols).map(|(h, d)| format!("{h}: {d}")).collect::<Vec<String>>().join(", ");
        Box::leak(out.into_boxed_str())
    }
}

/// Transposes a vector of Series
fn transpose(mat: &Vec<Series>) -> Vec<Series> {
    if mat.len() == 0 { return mat.to_vec() }
    (0..mat[0].size()).into_par_iter()
        .map(|i| {
        Series::new( mat.par_iter()
                        .map(|c| c.iloc(i))
                        .collect() 
                   )    
    }).collect()
}

/// Reads a CSV file into a DataFrame
///
/// # Examples
/// ```
/// let df: DataFrame = dataframe::read_csv("example.csv");
/// println!("{}", df);
/// ```
#[pyfunction]
pub fn read_csv(filename: &str) -> DataFrame {
    // Read the entire file to a String
    let file = fs::read_to_string(filename).expect("Something went wrong when reading");
    // Split into lines
    let lines: Vec<&str> = file.par_lines().collect();
    // Extract header row
    let header_row: Vec<String> = (&lines[0]).par_split(',').map(|x| String::from(x)).collect();
    // Parse data into numeric values
    let data: Vec<Series> = (&lines[1..]).into_par_iter().map(|line| {
        Series::new(
            line.split(",").map(|elt| { // split has better performance than par_split here
                match elt.parse::<f64>() {
                    Ok(f) => f,
                    Err(_) => f64::NAN
                }
            }).collect()
        )
    }).collect();

    // Transpose to get columns
    let df_data = transpose(&data);
    let size = data.len() * df_data.len();

    DataFrame {
        header_row,
        cols: df_data,
        rows: data,
        size
    }
}

/// Reads CSV files from a specified folder into a Vector of DataFrames
///
/// # Examples
/// ```
/// let dfs: Vec<DataFrame> = dataframe::read_csv_from_folder("/home/my_data/");
/// let summed = dfs.iter().map(|d| d.sum(0)).collect();
/// ```
#[pyfunction]
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

/// Reads CSV files whose names match a specified pattern into a Vector of DataFrames
///
/// # Examples
/// ```
/// let dfs: Vec<DataFrame> = dataframe::read_csv_by_glob("/home/my_data/*SetA*");
/// let summed = dfs.iter().map(|d| d.sum(0)).collect();
/// ```
#[pyfunction]
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

/// Creates a DataFrame from a HashMap
///
/// # Examples
/// ```
/// use std::collections::HashMap;
/// let mut data_map: HashMap<String, Vec<f64>> = HashMap::new();
/// data_map.insert("Col1".to_string(), vec![1.0, 2.0, 3.0, 4.0]);
/// data_map.insert("Col2".to_string(), vec![10.0, 20.0, 30.0, 40.0]);
/// data_map.insert("Col3".to_string(), vec![100.0, 200.0, 300.0, 400.0]);
/// let df = dataframe::from_hashmap(data_map);
/// println!("{}", df);
/// ```
#[pyfunction]
pub fn from_hashmap(data_map: HashMap<String, Vec<f64>>) -> DataFrame {
    let header: Vec<String> = data_map.keys().map(|x| x.clone()).collect();
    let data: Vec<Series> = data_map.values().map(|x| Series::new(x.clone())).collect();
    DataFrame::new(data, Some(header))
}

impl Display for DataFrame {
    fn fmt(&self, f: &mut Formatter) -> Result {
        let mut table = Table::new();

        if self.rows.len() < 10 && self.cols.len() < 10 {
            table.add_row(Row::from(self.header_row.clone()));
            for row in &self.rows {
                let _ = table.add_row(Row::from(row.to_vec()));
            }
        }
        else {
            let n = self.rows.len();
            let mut header = vec![];
            let mut dots = vec!["...".to_string()];

            let mut header_start: Vec<String> = self.header_row[0..3].to_vec();
            let mut header_end: Vec<String> = self.header_row[self.header_row.len() - 3..self.header_row.len()].to_vec();

            header.extend(&mut header_start);
            header.extend(&mut dots);
            header.extend(&mut header_end);
            
            table.add_row(Row::from(header));

            for row in &self.rows[0..3] {
                let mut r: Vec<String> = vec![];
                let m = row.size();
                let start: Vec<String> = row.slice(0, 3).to_vec().iter().map(|x| format!("{x}")).collect();
                let end: Vec<String> = row.slice(m-3, m).to_vec().iter().map(|x| format!("{x}")).collect();
                r.extend(start);
                r.extend(dots.clone());
                r.extend(end);
                table.add_row(Row::from(r));
            }

            table.add_row(Row::from(vec!["..."; 7]));

            for row in &self.rows[n-3..n] {
                let mut r: Vec<String> = vec![];
                let m = row.size();
                let start: Vec<String> = row.slice(0, 3).to_vec().iter().map(|x| format!("{x}")).collect();
                let end: Vec<String> = row.slice(m-3, m).to_vec().iter().map(|x| format!("{x}")).collect();
                r.extend(start);
                r.extend(dots.clone());
                r.extend(end);
                table.add_row(Row::from(r));
            }


        }
        //let out: Vec<String> = self.header_row.iter().zip(&self.cols).map(|(h, d)| format!("{h}: {d}")).collect();
        table.printstd();
        Ok(())
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

macro_rules! from_vec_type {
    ($type:ty) => {
        impl From<Vec<$type>> for DataFrame {
            fn from(data: Vec<$type>) -> Self {
                DataFrame::new(vec![Series::from(data)], None)
            }
        }
    }
}

macro_rules! from_2d_vec_type {
    ($type:ty) => {
        impl From<Vec<Vec<$type>>> for DataFrame {
            fn from(data: Vec<Vec<$type>>) -> Self {
                let rows = data.iter().map(|x| Series::from(x)).collect();
                let cols = transpose(&rows);
                let size = rows.len() * cols.len();
                let headers = DataFrame::gen_default_header(
                    rows.get(0).unwrap_or(&Series::zero()).size()
                );

                DataFrame {
                   header_row: headers,
                   cols,
                   rows,
                   size
                }

            }
        }
    }
}

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

from_2d_vec_type!(f64);
from_2d_vec_type!(f32);
from_2d_vec_type!(i8);
from_2d_vec_type!(i16);
from_2d_vec_type!(i32);
from_2d_vec_type!(i64);
from_2d_vec_type!(u8);
from_2d_vec_type!(u16);
from_2d_vec_type!(u32);
from_2d_vec_type!(u64);
