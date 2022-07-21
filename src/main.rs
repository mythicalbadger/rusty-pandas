extern crate num_traits;
extern crate rayon;
extern crate glob;
#[macro_use] extern crate prettytable;
mod funky_functions;
mod dataframe;
mod series;
use std::time::Instant;

fn gen_vec(n: usize) -> Vec<f64> {
    use rand::Rng;
    use rand::distributions::Standard;
    let rng = rand::thread_rng();
    rng.sample_iter(&Standard).take(n).collect()
}

fn main() {
    use series::Series;
    use dataframe::DataFrame;
    let df = dataframe::read_csv("./res/Exp_EverythingCells.csv");
    println!("{}", df);
}
