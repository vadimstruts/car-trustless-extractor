mod extractor;
mod error;
mod utils;
mod pb;

use std::path::PathBuf;
use async_std::task;
use extractor::{extract_by_mask, extract_all, extract_by_cid};
use rs_car::Cid;
use std::{ffi::CStr};
use std::os::raw::c_char;


#[no_mangle]
pub extern "C" fn extract_all_car(
    car_path: *const c_char,
    output_path: *const c_char
) -> bool {
   let car_path_str = unsafe { CStr::from_ptr(car_path) };
    let output_path_str = unsafe { CStr::from_ptr(output_path) };

    task::block_on(async {
        let result = extract_verified_all_from_car_async(
            car_path_str.to_string_lossy().to_string(),
            output_path_str.to_string_lossy().to_string(),
        )
        .await;
        match result {
            Ok(_) => return true,
            Err(err) => {
                eprintln!("Error: {}", err);
                return false;
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn extract_file_car(
    car_path: *const c_char,
    filter_file_pattern: *const c_char,
    output_path: *const c_char
) -> bool {
   let car_path_str = unsafe { CStr::from_ptr(car_path) };
    let filter_file_pattern_str = unsafe { CStr::from_ptr(filter_file_pattern) };
    let output_path_str = unsafe { CStr::from_ptr(output_path) };

    task::block_on(async {
        let result = extract_verified_file_from_car_async(
            car_path_str.to_string_lossy().to_string(),
            filter_file_pattern_str.to_string_lossy().to_string(),
            output_path_str.to_string_lossy().to_string(),
        )
        .await;
        match result {
            Ok(_) => return true,
            Err(err) => {
                eprintln!("Error: {}", err);
                return false;
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn extract_verified_by_cid_from_car(
    car_path: *const c_char,
    cid: *const c_char,
    output_path: *const c_char
) -> bool {
   let car_path_str = unsafe { CStr::from_ptr(car_path) };
    let cid_str = unsafe { CStr::from_ptr(cid) };
    let output_path_str = unsafe { CStr::from_ptr(output_path) };

    task::block_on(async {
        let result = extract_verified_by_cid_from_car_async(
            car_path_str.to_string_lossy().to_string(),
            cid_str.to_string_lossy().to_string(),
            output_path_str.to_string_lossy().to_string(),
        )
        .await;
        match result {
            Ok(_) => return true,
            Err(err) => {
                eprintln!("Error: {}", err);
                return false;
            }
        }
    })
}

async fn extract_verified_file_from_car_async(
    car_path: String,
    filter_file_pattern: String,
    output_path: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut input = async_std::fs::File::open(car_path).await?;
    let mut out_path = PathBuf::from(output_path);
    match extract_by_mask(&mut input, &mut out_path, 
        Some(filter_file_pattern)).await {
        Ok(_) => println!("Ok"),
        Err(err) => println!("Error: {}", err),
    }
    Ok(())
}

async fn extract_verified_all_from_car_async(
    car_path: String,
    output_path: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut input = async_std::fs::File::open(car_path).await?;
    let mut out_path = PathBuf::from(output_path);
    match extract_all(
        &mut input, 
        &mut out_path).await {
        Ok(_) => println!("Ok"),
        Err(err) => println!("Error: {}", err),
    }
    Ok(())
}

async fn extract_verified_by_cid_from_car_async(
    car_path: String,
    cid_str: String,
    output_path: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut input = async_std::fs::File::open(car_path).await?;
    let mut out_path = PathBuf::from(output_path);
    let cid = Cid::try_from(cid_str)?;
    match extract_by_cid(&mut input, &mut out_path, 
        Some(cid)).await {
        Ok(_) => println!("Ok"),
        Err(err) => println!("Error: {}", err),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    // #[test]
    // fn validate_carv1_test() {
    //     // Fixture has been taken from https://ipld.io/specs/transport/car/fixture/
    //     //let c_string = CString::new("/home/vadym/Projects/ipfs/ipfs-validation-go/go-car-test/dag.car").expect("Failed to create CString");
    //     let c_string = CString::new("test_files/carv1-basic.car").expect("Failed to create CString");
    //     let result = validate_car(c_string.as_ptr(), true);
    //     assert_ne!(result, false);
    // }
    // #[test]
    // fn validate_carv2_test() {
    //     // Fixture has been taken from https://ipld.io/specs/transport/car/fixture/
    //     //let c_string = CString::new("/home/vadym/Projects/ipfs/ipfs-validation-go/go-car-test/dag.car").expect("Failed to create CString");
    //     let c_string = CString::new("test_files/carv2-basic.car").expect("Failed to create CString");
    //     let result = validate_car(c_string.as_ptr(), true);
    //     assert_ne!(result, false);
    // }

    // #[test]
    // fn validate_carv2_test() {
    //     // Fixture has been taken from https://ipld.io/specs/transport/car/fixture/
    //     //let c_string = CString::new("/home/vadym/Projects/ipfs/ipfs-validation-go/go-car-test/dag.car").expect("Failed to create CString");
    //     let c_string = CString::new("test_files/two_folders_and_files.car").expect("Failed to create CString");
    //     let result = validate_car(c_string.as_ptr(), true);
    //     assert_ne!(result, false);
    // }

    #[test]
    fn extract_file_car_test() {
        let car_path =
            CString::new("/home/vadym/Projects/rust/rs-car-lib/rs-car-lib/test_files/test.car").expect("Failed to create CString");
        let filter_file_pattern_string = CString::new("AD299.txt")
            .expect("Failed to create CString");
        let output_path_string =
            CString::new("/home/vadym/Projects/rust/rs-car-lib/rs-car-lib/test_files/out_files").expect("Failed to create CString");
        let result = extract_file_car(
            car_path.as_ptr(),
            filter_file_pattern_string.as_ptr(),
            output_path_string.as_ptr()
        );
        assert_eq!(result, true);
    }
}
