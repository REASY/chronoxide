use smallvec::SmallVec;
use std::mem::size_of;

fn main() {
    println!("element_size_bytes={}", size_of::<f64>());
    report_vec();
    report_smallvec::<1>();
    report_smallvec::<2>();
    report_smallvec::<4>();
    report_smallvec::<8>();
    report_smallvec::<16>();
}

fn report_vec() {
    let struct_size = size_of::<Vec<f64>>();
    println!("\nVec<f64> struct_size_bytes={struct_size}");
    for len in 1..=10 {
        let mut v = Vec::with_capacity(len);
        for i in 0..len {
            v.push(i as f64);
        }
        let heap_bytes = v.capacity().saturating_mul(size_of::<f64>());
        let total = struct_size.saturating_add(heap_bytes);
        println!(
            "len={len:2} cap={:2} heap_bytes={:3} total_est_bytes={}",
            v.capacity(),
            heap_bytes,
            total
        );
    }
}

fn report_smallvec<const N: usize>() {
    let struct_size = size_of::<SmallVec<[f64; N]>>();
    println!("\nSmallVec<[f64; {}]> struct_size_bytes={struct_size}", N);
    for len in 1..=10 {
        let mut v: SmallVec<[f64; N]> = SmallVec::new();
        for i in 0..len {
            v.push(i as f64);
        }
        let spilled = v.spilled();
        let heap_bytes = if spilled {
            v.capacity().saturating_mul(size_of::<f64>())
        } else {
            0
        };
        let total = struct_size.saturating_add(heap_bytes);
        println!(
            "len={len:2} cap={:2} spilled={} heap_bytes={:3} total_est_bytes={}",
            v.capacity(),
            spilled,
            heap_bytes,
            total
        );
    }
}
