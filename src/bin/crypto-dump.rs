// this use statement gets you access to the lib file
use slurper::*;

use log::{info,debug,warn,error};
use std::error::Error;
use self::models::{CryptoTrade,CryptoMarket,RangeBoundAggregationSummary,TimeRange};

use linfa::traits::Predict;
use linfa::DatasetBase;
use linfa_clustering::{KMeans};

use ndarray::Array;
use ndarray::{Axis, array};
use ndarray_rand::rand::SeedableRng;
use rand_isaac::Isaac64Rng;


use futures::stream::TryStreamExt;
use mongodb::{Collection};
use mongodb::{Client};
use mongodb::{bson::doc};
use mongodb::options::{FindOptions};

use futures::join;

use std::iter::FromIterator;
use std::collections::HashMap;

use std::fmt; // Import `fmt`

#[macro_use]
extern crate clap;
use clap::App;

extern crate serde;
#[macro_use]
extern crate serde_derive;

extern crate csv;
use csv::Writer;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {

    env_logger::init(); 

    let yaml = load_yaml!("../coinmetrics.yml");
    let matches = App::from_yaml(yaml).get_matches();

    // Calling .unwrap() is safe here because "INPUT" is required (if "INPUT" wasn't
    // required we could have used an 'if let' to conditionally get the value)
    info!("processing on directive input: {}", matches.value_of("INPUT").unwrap());

    let client = Client::with_uri_str(LOCAL_MONGO).await?;
    let database = client.database(THE_DATABASE);
    let collection = database.collection::<CryptoTrade>(THE_CRYPTO_COLLECTION);
    let rbascollection = database.collection::<RangeBoundAggregationSummary>(THE_CRYPTO_RBAS_COLLECTION);

    // let time_ranges = get_time_ranges("2021-09-13 00:00:00","2021-09-14 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap();
    // let time_ranges = get_time_ranges("2021-09-14 00:00:00","2021-09-15 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap();


    match matches.value_of("INPUT").unwrap() {


        "usd-eth-spot-rbas" => {

            let mut wtr = Writer::from_path("/tmp/usd-eth-spot-rbas.csv")?;
            let filter = doc! {};
            let find_options = FindOptions::builder().sort(doc! { "gtedate":1, "aggregation_summary._id": 1}).build();
            let mut cursor = rbascollection.find(filter, find_options).await?;
            while let Some(rbas) = cursor.try_next().await? {
                println!("{}",rbas);
                let market = CryptoMarket {
                    market: rbas.aggregation_summary._id.clone()
                };
                if market.drop_all_but_instrument_type().unwrap() == "spot" {
                    wtr.serialize(rbas.get_csv().unwrap())?;                                                                            
                }
            }
            wtr.flush()?;
        },

        _ => {
            error!("i am tbd");
        }
    };

    Ok(())

}







#[cfg(test)]
mod tests {
    use std::{fs};
//    use std::fs::File;

    #[test]
    fn it_works_test_file_exists() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn it_works_can_open_close_file() {
        assert_eq!(3 + 3, 6);
    }


}












