use async_std::fs::{self, OpenOptions};
use futures::{AsyncRead, StreamExt, AsyncWriteExt};
use rs_car::{CarReader, Cid};
use std::{collections::HashMap, path::PathBuf};

use crate::{
    error::ReadSingleFileError,
    pb::{FlatUnixFs, PBLink},
    utils::{assert_header_single_file, hash_to_cid, links_to_cids},
};

pub async fn extract_all<R: AsyncRead + Send + Unpin>(
    car_input: &mut R, 
    out_path: &mut PathBuf
) -> Result<(), ReadSingleFileError> {
    extract(car_input, out_path, None, None, None).await
}

pub async fn extract_by_mask<R: AsyncRead + Send + Unpin>(
    car_input: &mut R, 
    out_path: &mut PathBuf,
    filter_file_pattern: Option<String>, // path + file name to extract
) -> Result<(), ReadSingleFileError> {
    extract(car_input, out_path, filter_file_pattern, None, None).await
}

pub async fn extract_by_cid<R: AsyncRead + Send + Unpin>(
    car_input: &mut R, 
    out_path: &mut PathBuf,
    cid: Option<Cid>,
) -> Result<(), ReadSingleFileError> {
    extract(car_input, out_path, None, cid, None).await
}


enum UnixFsNode {
    Links(Vec<Cid>),
    Data(Vec<u8>),
}

struct UnixFsNodeExt {
    node: UnixFsNode,
    name: String,
    is_file: bool
}

impl UnixFsNodeExt {
    // Constructor-like function
    fn new(node: UnixFsNode, name: String, is_file: bool) -> Self {
        Self {
            name: name,
            node: node,
            is_file: is_file,
        }
    }
}

/// Read CAR stream from `car_input` as a single file buffering the block dag in memory and
/// extracts sindle file filtered by pattern `filter_file_pattern` file to the base path `out_path`
/// ```
async fn extract<R: AsyncRead + Send + Unpin>(
    car_input: &mut R,
    out_path: &mut PathBuf,
    filter_file_pattern: Option<String>, // path + file name to extract
    start_cid: Option<Cid>,              // CID to extract
    max_buffer: Option<usize>,
) -> Result<(), ReadSingleFileError> {
    let mut streamer = CarReader::new(car_input, true).await?;

    // Optional verification of the root_cid
    let root_cid = assert_header_single_file(&streamer.header, start_cid.as_ref())?;

    println!(
        "root_cid:{} CODE:{}",
        root_cid.to_string(),
        root_cid.hash().code()
    );

    // In-memory buffer of data nodes
    let mut nodes = HashMap::new();
    let mut buffered_data_len: usize = 0;
    let mut path_map: HashMap<Cid, String> = HashMap::new();

    // Can the same data block be referenced multiple times? Say in a file with lots of duplicate content
    while let Some(item) = streamer.next().await {
        let (cid, block) = item?;
        println!("cid:{}", cid.to_string());
        let inner = FlatUnixFs::try_from(block.as_slice())
            .map_err(|err| ReadSingleFileError::InvalidUnixFs(err.to_string()))?;

        if inner.links.is_empty() {
            // Leaf data node
            let data = inner.data.Data.ok_or(ReadSingleFileError::InvalidUnixFs(
                "unixfs data node has not Data field".to_string(),
            ))?;

            // Allow to limit max buffered data to prevent OOM
            if let Some(max_buffer) = max_buffer {
                buffered_data_len += data.len();
                if buffered_data_len > max_buffer {
                    return Err(ReadSingleFileError::MaxBufferedData(max_buffer));
                }
            }

            let name = path_map.get(&cid).unwrap();
            // TODO: Is it possible to prevent having to clone here?
            nodes.insert(
                cid,
                UnixFsNodeExt::new(UnixFsNode::Data(data.to_vec()), String::from(name), true),
            );
        } else {
            let is_file = inner.data.Type == crate::pb::UnixFsType::File;
            if nodes.is_empty() {
                let v = if is_file {
                    "file" //TODO if extracting car with only one file (wich means it doesn't contain filename) shoul not use out file name "file"
                } else {
                    "/"
                };
                path_map.insert(cid, String::from(v));
            }


            inner.links.iter().for_each(|f: &PBLink<'_>| {
                let name = f.Name.as_ref().unwrap().to_owned().to_string();
                let link_cid = hash_to_cid(f.Hash.as_ref().unwrap()).unwrap();
                println!("Collected cid: {:?} name: {}", link_cid, name);
                let val = match inner.data.Type {
                    crate::pb::UnixFsType::File => {
                        let r = match path_map.get(&cid) {
                            Some(v) => v,
                            None => "file",
                        };
                        r
                    },
                    _ => name.as_str(),
                };

                path_map.insert(link_cid, String::from(val));
            });
            let name = path_map.get(&cid).unwrap();

            // Intermediary node (links)
            nodes.insert(
                cid,
                UnixFsNodeExt::new(
                    UnixFsNode::Links(links_to_cids(&inner.links)?),
                    String::from(name),
                    is_file
                ),
            );
        };
    }

    let path = match start_cid.is_some() {
        true => "/",
        false => "",
    };

    for (data, p) in flatten_tree(&nodes, &root_cid, path)? {
        if filter_file_pattern.as_ref().is_some()
            && p.ends_with(filter_file_pattern.as_ref().unwrap().as_str())
        {
            write_extracted_file(out_path.clone(), &data, &p).await?
        } else if start_cid.is_some() && start_cid.unwrap() == root_cid {
            write_extracted_file(out_path.clone(), &data, &p).await?
        } else {
            write_extracted_file(out_path.clone(), &data, &p).await?
        }
    }

    Ok(())
}

async fn write_extracted_file(
    mut out_path: PathBuf,
    data: &Vec<u8>,
    p: &String,
) -> Result<(), ReadSingleFileError> {
    out_path.push(p);
    println!(
        "!!!Found file: {} bytes: {}",
        out_path.display(),
        data.len()
    );

    fs::create_dir_all(out_path.parent().unwrap()).await?;

    let mut out = OpenOptions::new().append(true).create(true).open(&out_path).await?;

    out.write_all(&data)
        .await
        .map_err(|err| ReadSingleFileError::IoError(err))?;

    Ok(())
}

// TODO change it to not use recursion
fn flatten_tree<'a>(
    nodes: &'a HashMap<Cid, UnixFsNodeExt>,
    root_cid: &Cid,
    path: &str,
) -> Result<Vec<(&'a Vec<u8>, String)>, ReadSingleFileError> {
    let node = nodes
        .get(root_cid)
        .ok_or(ReadSingleFileError::MissingNode(*root_cid))?;

    Ok(match &node.node {
        UnixFsNode::Data(data) => vec![(data, node.name.clone())],
        UnixFsNode::Links(links) => {
            let mut out: Vec<(&'a Vec<u8>, String)> = vec![];
            let current_path = match path.is_empty() || path == "/" {
                true => node.name.clone(),
                false => match node.is_file {
                    false => format!("{}/{}", path, node.name.clone()),
                    true => "".to_owned(),
                }
            };
            for link in links {
                for (data, p) in flatten_tree(nodes, &link, current_path.clone().as_str())? {
                    match path.is_empty() {
                        true => out.push((data, p)),
                        false => {
                            out.push((data, format!("{}/{}", current_path.clone(), p.as_str())))
                        }
                    };
                }
            }
            out
        }
    })
}

#[cfg(test)]
mod tests {
    use async_std::fs::{self};
    use std::path::Path;
    use async_std::task;
    use super::*;

    fn car_file() -> &'static str {
        "/home/vadym/Projects/rust/rs-car-lib/rs-car-lib/test_files/test.car"
    }
    fn out_path() -> &'static str {
        "/home/vadym/Projects/rust/rs-car-lib/rs-car-lib/test_files/out_files/"
    }

    fn one_multiblock_item_car_file() -> &'static str {
        "/home/vadym/Projects/rust/rs-car-lib/rs-car-lib/test_files/dag.car"
    }
    async fn clear_output(path: &str) -> Result<(), std::io::Error> {
        let p = Path::new(path);
        if p.exists() {
            match fs::remove_dir_all(path).await {
                Ok(_) => {
                    fs::create_dir(path).await?
                },
                Err(err) => {
                    eprint!("Error: {:?}", err);
                    return Err(err);
                },
            }            
        }

        Ok(())
    }


    #[test]
    fn extract_all_car_test() {
        task::block_on(async {
            match clear_output(out_path()).await {
                Ok(_) => println!("Cleaned the: {}", out_path()),
                Err(err) => eprint!("Error:{:?}", err),
            }

            let mut input = async_std::fs::File::open(one_multiblock_item_car_file())
            .await
            .unwrap();
            let mut out_path = PathBuf::from(out_path());
            match extract_all(
                &mut input,
                &mut out_path)
            .await
            {
                Ok(_) => println!("Ok:"),
                Err(err) => println!("Error: {}", err),
            }
        })
    }

    #[test]
    fn extract_file_car_by_path_test() {
        task::block_on(async {
            match clear_output(out_path()).await {
                Ok(_) => println!("Cleaned the: {}", out_path()),
                Err(err) => eprint!("Error:{:?}", err),
            }

            let mut input = async_std::fs::File::open(car_file())
            .await
            .unwrap();
            let mut out_path = PathBuf::from(out_path());
            match extract_by_mask(
                &mut input,
                &mut out_path,
                Some("second/AD299.txt".to_owned()))
            .await
            {
                Ok(_) => println!("Ok:"),
                Err(err) => println!("Error: {}", err),
            }
        })
    }

    #[test]
    fn extract_file_car_by_path_all_txt_test() {
        task::block_on(async {
            match clear_output(out_path()).await {
                Ok(_) => println!("Cleaned the: {}", out_path()),
                Err(err) => eprint!("Error:{:?}", err),
            }
            let mut input = async_std::fs::File::open(
                "/home/vadym/Projects/rust/rs-car-lib/rs-car-lib/test_files/test.car",
            )
            .await
            .unwrap();
            let mut out_path = PathBuf::from(
                "/home/vadym/Projects/rust/rs-car-lib/rs-car-lib/test_files/out_files/",
            );
            match extract_by_mask(
                &mut input,
                &mut out_path,
                Some(".txt".to_owned()))
            .await
            {
                Ok(_) => println!("Ok:"),
                Err(err) => println!("Error: {}", err),
            }
        })
    }

    #[test]
    fn extract_file_car_by_cid_test() {
        task::block_on(async {
            match clear_output(out_path()).await {
                Ok(_) => println!("Cleaned the: {}", out_path()),
                Err(err) => eprint!("Error:{:?}", err),
            }
            let start_cid =
                Some(Cid::try_from("QmSobEqJTzVCPTPKyTkuYpS7MXsJj2tnwUN4bgdqPSL26M").unwrap());

            let mut input = async_std::fs::File::open(
                "/home/vadym/Projects/rust/rs-car-lib/rs-car-lib/test_files/test.car",
            )
            .await
            .unwrap();
            let mut out_path = PathBuf::from(
                "/home/vadym/Projects/rust/rs-car-lib/rs-car-lib/test_files/out_files/",
            );
            match extract_by_cid(&mut input, &mut out_path, start_cid).await {
                Ok(_) => println!("Ok:"),
                Err(err) => println!("Error: {}", err),
            }
        })
    }
}
