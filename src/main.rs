extern crate num_traits;
extern crate rayon;
extern crate glob;
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
    let a = Series::new(vec![1.0, 2.0, 3.0]);
    let b = Series::new(vec![4.0, -5.0, 6.0]);
    println!("{}", a.vsub(b));
}
