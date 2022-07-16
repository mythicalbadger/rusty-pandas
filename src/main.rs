extern crate num_traits;
extern crate rayon;
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
    /*
    let n = 10_000_000;
    let st = Instant::now();
    let test = gen_vec(n);
    let end = st.elapsed();
    println!("Generated a vector of {} numbers in {:.2?} seconds", n, end); 

    let df = dataframe::DataFrame::new(
        test
    );

    let st = Instant::now();
    let test_size = df.size();
    let end = st.elapsed();
    println!("Calculated size ({}) in {:.2?}", test_size, end); 

    let st = Instant::now();
    let test_sum = df.sum();
    let end = st.elapsed();
    println!("Calculated sum ({}) in {:.2?}", test_sum, end); 

    let st = Instant::now();
    let test_mean= df.mean();
    let end = st.elapsed();
    println!("Calculated mean ({}) in {:.2?}", test_mean, end); 

    let st = Instant::now();
    let test_min= df.min();
    let end = st.elapsed();
    println!("Calculated seq min ({}) in {:.2?}", test_min, end); 
    let filename = "res/Exp_EverythingCells.csv";
    let df = dataframe::read_csv(filename); 
    println!("{}", df);
    println!("{}", df.sum(0));
    println!("{}", df.sum(1));
    println!("{}", df.size);
    println!("{}", df.mean(0));
    println!("{}", df[0]);
    println!("{}", df.min(1));

    */
    let st = Instant::now();
    let dfs = dataframe::read_csv("./res/example.csv");
    let means = dfs.mean(0);
    means.to_csv("./res/cowabunga.csv");
    println!("{:?}", st.elapsed().as_secs());
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
