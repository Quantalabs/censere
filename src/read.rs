extern crate csv;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::thread;

use csv::{StringRecord, Writer};

pub struct Codes {
    pub languages: Vec<String>,
    pub ancestry: Vec<String>,
}

pub struct FilterLang(pub String, pub Codes);

pub fn read<P: AsRef<Path>>(file: P, cols: Vec<usize>, num: &'static usize, start: &'static usize) -> Result<(Vec<Vec<String>>, Vec<String>), Box<dyn Error>> {
    let f = File::open(file)?;
    let mut rdr = csv::ReaderBuilder::new().from_reader(f);
    let headers = rdr.headers()?.clone();
    let records: Vec<StringRecord> = rdr.records().filter_map(|x| if x.is_ok() { Some(x.unwrap()) } else { None }).collect();
    let clone: Vec<StringRecord> = records.clone();
   
    let first_half =  &(records.clone())[(*start)..(((*num as f64/2.0).ceil() as usize) + *start)];    
    let cols_clone = cols.clone();
    
    let handle = thread::spawn(move || {
        (&clone[(((*num as f64/2.0).ceil() as usize) + *start)..(*num + *start)]).iter().map(
            |x| x.iter().enumerate().filter(|(i, _)| cols_clone.contains(i)).map(|(_, j)| String::from(j)).collect()
        ).collect()
    });
    
    let mut values: Vec<Vec<String>> = first_half.iter().map(
        |x| x.iter().enumerate().filter(|(i, _)| cols.contains(i)).map(|(_, j)| String::from(j)).collect()
    ).collect();

    let mut valuesb = handle.join().unwrap();

    values.append(&mut valuesb);
    
    Ok((values, headers.iter().enumerate().filter(|(i, _)| cols.contains(i)).map(|(_, j)| String::from(j)).collect()))
}

/** Single-Threaded Implementation of read() */
pub fn read_single<P: AsRef<Path>>(file: P, cols: Vec<usize>) -> Result<(Vec<Vec<String>>, Vec<String>), Box<dyn Error>> {
    let f = File::open(file)?;
    let mut rdr = csv::ReaderBuilder::new().from_reader(f);
    let headers = rdr.headers()?.clone();
    let records: Vec<StringRecord> = rdr.records().filter_map(|x| if x.is_ok() { Some(x.unwrap()) } else { None }).collect();

    let values: Vec<Vec<String>> = records.iter().map(
        |x| x.iter().enumerate().filter(|(i, _)| cols.contains(i)).map(|(_, j)| String::from(j)).collect()
    ).collect();
  
    Ok((values, headers.iter().enumerate().filter(|(i, _)| cols.contains(i)).map(|(_, j)| String::from(j)).collect()))
}

pub fn filter<T: Fn(&Vec<String>) -> bool>(data: &Vec<Vec<String>>, qualifier: T) -> Vec<&Vec<String>>
{
    data.iter().filter(|x| qualifier(*x)).collect()
}

pub fn export<P: AsRef<Path>>(file: P, data: Vec<&Vec<String>>, header: &Vec<String>) -> Result<(), Box<dyn Error>> {
    let mut wtr = Writer::from_path(file)?;

    wtr.write_record(header)?;
    data.iter().for_each(|x| wtr.write_record(*x).unwrap());

    wtr.flush()?;

    Ok(())
}

pub fn filter_metro(metro: String, filters: &Vec<FilterLang>, num: &'static usize, start: &'static usize) {
    let columns: Vec<usize>  = (0..12).collect();
    let (data, headers) = read(format!("./data/raw/{}.csv", metro), columns.clone(), num, start).unwrap();
    
    let header: Vec<String> = headers.iter()
        .enumerate()
        .filter(|(i, _)| columns.contains(i)).map(|x| x.1.clone())
        .collect();
  
    for f in filters.iter() {
        let data = filter(&data, |x| {
                f.1.languages.contains(&x[10])
                || f.1.ancestry.contains(&x[5])
                || f.1.ancestry.contains(&x[7])
        });
    
        let _ = export(format!("./data/{}/{}.csv", metro, f.0), data, &header);
    }
}
