# Rusty Pandas
![Rusty Panda????](https://i.imgur.com/jbDV1Eb.jpg)
## Project Description
Rusty Pandas is a final project for my university's ICCS311 Functional and Parallel Programming class. As the name implies, the project is an attempt at a lesser, parallelized cousin of Pandas's DataFrame. Here is a semi-paraphrased version of the project proposal that was submitted.
### Abstract
Pandas is no doubt one of the more popular Python libraries out there; praised for its ease of use, flexibility and sheer power (and perhaps soft, cuddly appearance?) 

Despite its functionality, many have argued that the library itself is *slow*. This makes sense -- DataFrames are, after all, built on top of NumPy. A lot of internal bookkeeping happens behind the scenes to keep things running smoothly which, although small in cost, can add up (it may also be because bamboo is not a very good source of nutrients, leaving pandas sluggish). Another reason might be due to lack of parallelization in the library itself. 

Therefore, in the pursuit of speed, power and glory, I aim to port a lesser version of Pandas over to Rust and parallelize a bunch of its core features in the process.
### Objectives
The objectives for the project are as follows
1. Port the Pandas DataFrame -- or some lesser cousin of it -- to Rust. More specifically, this would include coming up with a Rust version of a Series, providing the overall shape and feel of a DataFrame (rows/columns/indices) and supporting a majority of the core functionality of DataFrames (I'd say about 70\%-80\% of the methods listed on [this page](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html))
2. Optimize and parallelize a subset of these core operations and hopefully see an increase in speed (for larger datasets at least). I assume it would be both a combination of sequential and parallel, depending on the size of the data being operated on.
3. Create a Python wrapper for the library to allow ease of use using `PyO3`

All in all, I believe they are achievable goals (unless Algorithms and Tractability annihilates me).
## Project Outcomes
The outcome of the project was quite different from what I had originally imagined.  A lot of time was wasted fighting with Rust about types, determining the best way to represent data. Eventually, in order to begin the actual process of working on the code, I had to scope the DataFrame down from storing heterogeneous data to homogeneous data (in this case, Rust's `f64`s) with a header row. Something along the lines of the table below 

| Column 1 | Column 2 | Column 3| ...
| --- | :---: | :---: | :---: |
| `3.141` | `0.577` | `42.0` | ...
| `1.618` | `1.414` | `2.718` | ...
| ... | ... |... | ...

Although this constrained the DataFrames to strictly numerical values, it allowed me to stop fighting Rust to make generics work and just begin working on the functionality.
### Structure
Rusty Pandas is split into two main pieces

- The `Series` type, which acts as a `Vector` with extended capabilities 
- The `DataFrame` type, which acts as essentially a two-dimensional vector of `Series`

Most methods in the `DataFrame` work by essentially mapping over each `Series` and applying an operation on each, both in parallel.

Additionally, through the usage of `PyO3` (which was magical), there is also support for Python. 
### Functionality
Since a fair amount of time was squandered fighting with Rust, a lot of the functionality remains pretty basic/statistical in nature. It is far from the ambitious 70-80% originally planned in the project proposal. Nevertheless, I believe they provide a fairly solid foundation for more progress in the future. Here is the full list of methods, in no particular order.

For `Series`

| Method | Description| 
| :--- | :--- |
| `new(data: Vec<f64>) -> Series`| Creates a new Series
| `size() -> usize`| Returns the number of elements inside the Series
| `is_empty() -> bool` | Returns a Boolean indicating whether or not the Series is empty
| `iloc(idx: usize) -> f64`| Accesses a specific index inside the Series
| `sum() -> Series`| Sums the values inside the Series
| `prod() -> Series`| Computes the product of all values inside the Series
| `dropna() -> Series`| Returns a new Series with all non-numerical/NaN values filtered out
| `isna() -> Series`| Indicates indices with missing values
| `notna() -> Series`| Indicates existing (non-missing) values
| ~~`any(pred: fn(f64) -> bool) -> bool`~~| ~~Computes the product of all values inside the Series~~ *(Removed cause `PyO3` didn't like)*
| `sort() -> Series`| Sorts the series
| `mean() -> Series`| Calculates the mean of the values inside the Series 
| `median() -> Series`| Calculates the median of the values inside the Series 
| `mode() -> Series`| Calculates the mode of the values inside the Series 
| `var() -> Series`| Calculates the variance of the values inside the Series 
| `std() -> Series`| Calculates the standard deviation of the values inside the Series 
| `min() -> Series`| Calculates the minimum of the values inside the Series 
| `max() -> Series`| Calculates the maximum of the values inside the Series 
| ~~`apply(f: fn(f64) -> f64) -> Series`~~|~~Applies a function to all elements~~ *(Removed cause `PyO3` didn't like)*
| `plus(n: f64) -> Series`| Element wise addition
| `sub(n: f64) -> Series`| Element wise subtraction
| `mult(n: f64) -> Series`| Element wise multiplication
| `div(n: f64) -> Series`| Element wise division
| `cumsum() -> Series`| Calculates the cumulative/prefix sum of a Series
| `join(token: &str) -> String`| Joins the Series into string
| `slice(start: usize, end: usize) -> String`| Extracts a slice from the Series
| `dot(other: Series) -> Series`| Computes the dot product of the Series and another
| `vadd(other: Series) -> Series`| Computes the vector sum of the Series and another
| `vsub(other: Series) -> Series`| Computes vector subtraction of the Series and another
| `norm() -> Series`| Computes norm/magnitude of the Series
| `to_vec() -> Vec<f64>`| Converts the Series to a `Vector` of `f64`

For the `DataFrame` object and `dataframe` module

| Method | Description| 
| :--- | :--- |
| `new(data: Vec<Series>, header_row: Option<Vec<String>>) -> DataFrame`| Creates a new DataFrame
|`irow(row: usize) -> Series`| Extracts a row from the DataFrame by index
|`icol(row: usize) -> Series`| Extracts a column from the DataFrame by index
|`loc_col(col_name: &str) -> Option<Series>`| Extracts a column from the DataFrame by name/header
| `size() -> usize`| Returns the number of elements inside the DataFrame
| `dropna() -> DataFrame`| Drops any rows/columns that contain missing values
| `dropnull() -> DataFrame` | Alias for `dropna`
| `sum(axis: usize) -> DataFrame`| Sums each Series in the DataFrame across an axis
| `prod(axis: usize) -> DataFrame`| Computes the product over values for each Series in the DataFrame across an axis
| `mean(axis: usize) -> DataFrame`| Computes the mean for each Series in the DataFrame across an axis
| `median(axis: usize) -> DataFrame`| Computes the median for each Series in the DataFrame across an axis
| `mode(axis: usize) -> DataFrame`| Computes the mode for each Series in the DataFrame across an axis
| `var(axis: usize) -> DataFrame`| Computes the variance for each Series in the DataFrame across an axis
| `std(axis: usize) -> DataFrame`| Computes the standard deviation for each Series in the DataFrame across an axis
| `min(axis: usize) -> DataFrame`| Computes the minimum for each Series in the DataFrame across an axis
| `max(axis: usize) -> DataFrame`| Computes the maximum for each Series in the DataFrame across an axis
| ~~`apply(f: fn(f64) -> f64) -> DataFrame`~~|~~Applies a function to each Series in the DataFrame across an axis~~ *(Removed cause `PyO3` didn't like)*
| `copy() -> DataFrame`| Creates a deepcopy of a DataFrame
| `to_csv() -> ()`| Writes the contents of the DataFrame to a CSV file
| `to_hashmap() -> std::collections::HashMap<String, Vec<f64>>`| Converts the DataFrame to a Rust `HashMap`
| `head(n: usize) -> DataFrame`| Extracts the first `n` rows of the DataFrame
| `tail(n: usize) -> DataFrame`| Extracts the last `n` rows of the DataFrame
| `plus(n: f64) -> DataFrame`| Adds a value to all elements in the DataFrame
| `sub(n: f64) -> DataFrame`| Subtracts a value from all elements in the DataFrame
| `mult(n: f64) -> DataFrame`| Multiplies a value to all elements in the DataFrame
| `div(n: f64) -> DataFrame`| Divides all elements in the DataFrame by a value
| `cumsum(axis: usize) -> DataFrame`| Computes the cumulative/prefix sum for each Series in the DataFrame over an axis
| `insert_col(pos: usize, column_name: &str, column: Series) -> DataFrame`| Returns a new DataFrame with a new column inserted into it 
|`read_csv(filename: &str) -> DataFrame`| Reads a CSV file into a DataFrame
|`read_csv_from_folder(folder_name: &str) -> Vec<DataFrame>`| Reads CSV files from a specified folder into a Vector of DataFrames
`read_csv_by_glob(path: &str, expr: &str) -> Vec<DataFrame>`| Reads CSV files whose names match a specified pattern into a Vector of DataFrames
|`from_hashmap(data_map: std::collections::HashMap<String, Vec<f64>>) -> DataFrame`| Creates a DataFrame from a Rust `HashMap`

A lot of these still have room for improvement. The two also implement the following traits

For `Series`

| Trait | Supported Types | 
| :--- | :--- |
| `From`| `Series` can be constructed from `T`, `Vec<T>`, `Range<T>`, and `RangeInclusive<T>` where `T: {f32, f64, i8, i16, i32, i64, u8, u16, u32, u64}`
| `Add`| `Series` can be concatenated through the `Add` trait
| `Zero` | Through the utilization of the `num_traits` crate, an empty `Series` can be created using `Series::zero()`
| `PartialEq` / `Eq` | `Series` can be compared for equality through the `Eq` trait.
| `Display` | `Series` can be displayed properly through the `Display` trait

For `DataFrame`

| Trait | Supported Types | 
| :--- | :--- |
| `From`| `DataFrame`s can be constructed from `Vec<T>` and `Vec<Vec<T>>` where `T: {f32, f64, i8, i16, i32, i64, u8, u16, u32, u64}`
| `PartialEq` / `Eq` | `DataFrame`s can be compared for equality through the `Eq` trait.
| `Display` | `DataFrame`s can be displayed in table form with help from the `prettytable` crate using `Display` trait

## Benchmarks
### Setup
The following 'benchmarks' (if you can call them that) were performed on an Asus Zenbook UX533FN with an Intel i7-8565U CPU with only NeoVim open. The CSV used was a spreadsheet containing flight data from 2008 with 29 columns and 7009729 rows that was also used for an assignment in class. There was no cherry picking, just a guy running code and putting the first ten outputs into a spreadsheet. The code for the benchmarks can be found in`benchmarks/2008_test.py`. 

There are a couple of differences that make Pandas and Rusty Pandas hard to compare -- most notably, the fact that Rusty Pandas only handles numeric data. That means if a spreadsheet contains strings, Rusty Pandas could compute the sum over an axis in under a second, while Pandas would take longer than 30 minutes (practically an "infinite speedup"). Therefore, to counteract this issue, Pandas operations were run with the `numeric_only=True` flag.
### Insights
Upon conducting some basic benchmarks, two things became clear
- When datasets were small, Rusty Pandas tended to be slower or the same speed as Pandas. This was to be expected, due to parallel overhead.
- Of more interest perhaps, was that Rusty Pandas tended to perform much better with column operations than it did row operations
I believe this can be chalked up to the fact that Rusty Pandas is primarily designed with spreadsheets that have a huge number of rows in mind (sounds counter-intuitive when I say it works better with column operations but it really isn't). If there are seven million rows, something like summing over columns allows for us to take greater advantage of parallelism.

There were also some places where performance was worse, for example, element wise operations, or computing the dot product of Series. For these, I believe it would be beneficial to do some performance engineering (SIMD for dot product perhaps, although that might be slower).
### Data
A spreadsheet containing some basic benchmarks [can be found here](https://docs.google.com/spreadsheets/d/1ZpH7RMpotfpuGFky_-1ls36aShXh50yD_bOaytv7yls/edit?usp=sharing). They are far from comprehensive or professional. Many functions are missing, because, as noted earlier, differences in structure make the two a bit hard to compare, and not all Pandas functions have the `numeric_only` flag.
