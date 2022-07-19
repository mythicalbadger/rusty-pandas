extern crate num_traits;
extern crate rayon;
extern crate glob;
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
    let st = Instant::now();
    println!("Reading");
    let _df = dataframe::read_csv("./res/2008.csv");
    println!("{:?} to read", st.elapsed().as_secs());
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
