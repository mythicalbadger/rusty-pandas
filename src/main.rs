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
    /*
    let st = Instant::now();
    println!("Reading");
    let df = dataframe::read_csv("./res/example.csv");
    println!("{:?} to read", st.elapsed().as_secs());


    let st = Instant::now();
    let first = df.head(1);
    let second = df.tail(1);

    println!("{:?} to head", st.elapsed().as_secs());
    println!("{}", first);
    println!("{}", second);
    */
    use dataframe::DataFrame;
    use series::Series;
    use std::collections::HashMap;
    let mut data_map: HashMap<String, Vec<f64>> = HashMap::new();
    data_map.insert("Col1".to_string(), vec![1.0, 2.0, 3.0, 4.0]);
    data_map.insert("Col2".to_string(), vec![10.0, 20.0, 30.0, 40.0]);
    data_map.insert("Col3".to_string(), vec![100.0, 200.0, 300.0, 400.0]);
    let df = dataframe::from_hashmap(data_map);
    println!("{}", df);

}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
