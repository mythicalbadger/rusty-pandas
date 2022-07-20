#[allow(dead_code)]
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use rayon::prelude::*;
use crate::series::*;
use std::cell::UnsafeCell;

/// Transposes a vector of Series
fn transpose(mat: &Vec<Series>) -> Vec<Series> {
    if mat.len() == 0 { return mat.to_vec() }
    (0..mat[0].size()).into_par_iter()
        .map(|i| {
        Series::new( mat.par_iter()
                        .map(|c| c.iloc(i))
                        .collect() 
                   )    
    }).collect()
}

#[derive(Copy, Clone)]
pub struct UnsafeSlice<'a, T> {
    slice: &'a [UnsafeCell<T>],
}
unsafe impl<'a, T: Send + Sync> Send for UnsafeSlice<'a, T> {}
unsafe impl<'a, T: Send + Sync> Sync for UnsafeSlice<'a, T> {}

impl<'a, T> UnsafeSlice<'a, T> {
    pub fn new(slice: &'a mut [T]) -> Self {
        let ptr = slice as *mut [T] as *const [UnsafeCell<T>];
        Self {
            slice: unsafe { &*ptr },
        }
    }
    
    /// SAFETY: It is UB if two threads write to the same index without
    /// synchronization.
    pub unsafe fn write(&self, i: usize, value: T) {
        let ptr = self.slice[i].get();
        *ptr = value;
    }
}

// Pulled from lecture notes
fn prefix_sum(xs: &[i32]) -> (Vec<i32>, i32) {
    if xs.is_empty() { return (vec![], 0); }

    // Speeds it up quite a bit
    if xs.len() < 8192 {
        let mut pfs = vec![0];
        for i in 0..xs.len() {
            pfs.push(xs[0..i+1].iter().sum());
        }
        return (pfs[0..pfs.len()-1].to_vec(), pfs[pfs.len()-1])
    }

    let half = xs.len() / 2;
    let (c_prefix, mut c_sum) = prefix_sum(
        &(0..half).into_par_iter()
            .map(|i| xs[i*2] + xs[i*2+1])
            .collect::<Vec<i32>>()
    );

    let mut pfs: Vec<i32> = (0..half).into_par_iter()
        .flat_map(|i| vec![c_prefix[i], c_prefix[i]+xs[2*i]])
        .collect();

    if xs.len() % 2 == 1 { pfs.push(c_sum); c_sum += xs[xs.len() - 1]; }
    
    (pfs, c_sum)
}

#[allow(dead_code)]
pub fn par_filter<F>(xs: Vec<(usize, char)>, p: F) -> Vec<usize>
where 
    F: Fn((usize, char)) -> bool + Send + Sync
{
    // Apply p to xs and convert boolean to their numerical equivalent
    let binary_xs: Vec<i32> = xs.par_iter().map(|&x| p(x) as i32).collect();
    // Run prefix sum to determine the total number of elements after filter / their indices
    let (indices, n_filtered) = prefix_sum(&binary_xs);

    // Set up our output array -- we know the number of elements from p_sum
    let mut output: Vec<usize> = Vec::with_capacity(n_filtered as usize);
    unsafe { output.set_len(n_filtered as usize); }
    let output_us = UnsafeSlice::new(&mut output);

    // Fill in the output in parallel using UnsafeSlice
    (0..xs.len()).into_par_iter().for_each(|i| {
        if binary_xs[i] == 1 {
            let out_idx = indices[i] as usize;
            let elem = xs[i];
            unsafe { output_us.write(out_idx, elem.0+1); }
        }
    });

    output
}

pub fn par_split<'a>(st_buf: &'a str, split_char: char) -> Vec<&'a str> {    
      // par_filter to find indices that are our split char then map over those indices    
      // This is quite slow compared to built in .par_split, but speedy enough with --release    
      let is_split_char = |elt: (usize, char)| elt.1 == split_char;
      let enumed: Vec<(usize, char)> = st_buf.chars().enumerate().collect(); 
      // Collect indices to start split slices    
      let mut indices = vec![0usize];
      indices.append(&mut par_filter(enumed, is_split_char));
      indices.push(st_buf.len() + 1);    
       
      indices.par_windows(2).map(|chunk| &st_buf[chunk[0]..chunk[1]-1]).collect()
}
