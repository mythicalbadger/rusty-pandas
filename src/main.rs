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
    let df = dataframe::read_csv("./res/2008.csv");
    println!("Summing across columns");
    let st = Instant::now();
    let a = df.sum(0);
    println!("{:?} to sum across columns", st.elapsed().as_millis());
    println!("Summing across rows");
    let st = Instant::now();
    let a = df.sum(1);
    println!("{:?} to sum across rows", st.elapsed().as_millis());

    println!("{}", a.size());
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
