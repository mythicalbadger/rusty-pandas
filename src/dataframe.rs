use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;

pub struct DataFrame<> {
    data: Vec<f64>,
}

impl std::fmt::Display for DataFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.data)
    }
}

impl DataFrame {
    pub fn new(data: Vec<f64>) -> DataFrame {
        DataFrame { data }
    }


    pub fn size(&self) -> i64 {
        self.data.len() as i64
    }

    pub fn sum(&self) -> f64 {
        if &self.size() < &1024 { (&self.data).iter().sum() }
        else {
            (&self.data)
                .into_par_iter()
                .sum()
        }
    }

    pub fn mean(&self) -> f64 {
        self.sum() / self.size() as f64
    }

    pub fn abs(&self) -> DataFrame {
        if self.size() < 1024 { 
            DataFrame::new((&self.data).iter()
                        .map(|x| x.abs())
                        .collect())
        }
        else { 
            DataFrame::new((&self.data).into_par_iter()
                        .map(|x| x.abs())
                        .collect())
        }
    }
}
