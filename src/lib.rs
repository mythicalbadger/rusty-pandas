use pyo3::prelude::*;
pub mod series;
pub mod dataframe;

pub use series::Series;
pub use dataframe::DataFrame;

#[pymodule]
fn rusty_pandas(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Series>()?;
    m.add_class::<DataFrame>()?;
    m.add_function(wrap_pyfunction!(dataframe::read_csv, m)?)?;
    m.add_function(wrap_pyfunction!(dataframe::read_csv_from_folder, m)?)?;
    m.add_function(wrap_pyfunction!(dataframe::read_csv_by_glob, m)?)?;
    m.add_function(wrap_pyfunction!(dataframe::from_hashmap, m)?)?;
    Ok(())
}
