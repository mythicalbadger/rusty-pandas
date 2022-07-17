extern crate num_traits;
extern crate rayon;
extern crate glob;
extern crate chashmap;
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
    use std::collections::HashMap;
    let mut test_map: HashMap<String, Vec<f64>> = HashMap::new();
    test_map.insert("Column 1".to_string(), vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    test_map.insert("Column 2".to_string(), vec![10.0, 20.0, 30.0, 40.0, 50.0]);
    test_map.insert("Column 3".to_string(), vec![100.0, 200.0, 300.0, 400.0, 500.0]);
    let df = dataframe::from_hashmap(test_map);
    println!("{}", df.max(0));
    let df_map = df.to_hashmap();
    println!("{:?}", df_map);
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
