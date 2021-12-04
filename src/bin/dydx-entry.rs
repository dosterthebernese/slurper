//! The entry point for interacting with the dydx exchange.  Invoke the main by calling the program with one of two options: 
//!
//! RUST_LOG=DEBUG cargo run --bin dydx-entry all-markets
//!
//! RUST_LOG=DEBUG cargo run --bin dydx-entry beta-consumer
//!
//! The all-markets invocation writes to a kafka topic, dydx (see readme for how to create), and the beta-consumer reads it, and does the kmeans calcs, writing to a csv in /tmp
//!
//! Known shittiness: the all-markets call dies after about 12 hours.  The consumer probably could also write the data to mongo, and truncate the kafka partition post consumption.collection


mod dydx;

use slurper::*;
use log::{info,debug,warn};
use std::error::Error;
//use std::convert::TryFrom;
use dydx::{TLDYDXMarket};
use self::models::{ClusterBomb};
use chrono::{DateTime,Utc,SecondsFormat};
use tokio::time as TokioTime;  //renamed norm duration so could use this for interval
use tokio::time::Duration as TokioDuration;  //renamed norm duration so could use this for interval
use std::collections::HashMap;

//use futures::stream::TryStreamExt;
//use mongodb::{Client};
//use mongodb::{bson::doc};
//use mongodb::options::{FindOptions};

use std::time::Duration;

//use kafka::error::Error as KafkaError;
use kafka::producer::{Producer, Record, RequiredAcks};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

use std::str;

#[macro_use]
extern crate clap;
use clap::App;

extern crate serde;
extern crate base64;


extern crate csv;
use csv::Writer;



/// Invoke dydx with one of the input options highlighted above.
#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {

    env_logger::init(); 


    let yaml = load_yaml!("../cmds.yml");
    let matches = App::from_yaml(yaml).get_matches();
//    debug!("{:?}",matches);

    // let client = Client::with_uri_str(LOCAL_MONGO).await?;
    // let database = client.database(THE_DATABASE);
    // let tlphsnapcollection = database.collection::<TLPhemexMDSnapshot>(THE_TRADELLAMA_PHEMEX_MD_SNAPSHOT_COLLECTION);
    // let lcollection = database.collection::<CryptoLiquidation>(THE_CRYPTO_LIQUIDATION_COLLECTION);

    // Calling .unwrap() is safe here because "INPUT" is required (if "INPUT" wasn't
    // required we could have used an 'if let' to conditionally get the value)
    info!("processing on directive input: {}", matches.value_of("INPUT").unwrap());

    match matches.value_of("INPUT").unwrap() {


        "beta-consumer" => {

            dydx::consume_dydx_topic().await?;
        },


        "all-markets" => {

            dydx::process_all_markets().await?;


        },
        _ => {
            debug!("Unrecognized input parm.");
        }


    }



    Ok(())

}







#[cfg(test)]
mod tests {
//    use std::{fs};
//    use std::fs::File;

    #[test]
    fn it_works_test_file_exists() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn it_works_can_open_close_file() {
        assert_eq!(3 + 3, 6);
    }

    #[test]
    fn trailing_vec_logic() {
        let mut tapering_vec: Vec<f64> = vec![1.,2.,3.,4.,5.,6.,7.,8.,9.,10.,11.,12.,13.,];
        assert_eq!(tapering_vec.len(), 13);
        assert_eq!(4.,tapering_vec[3]);
        tapering_vec.push(14.);
        assert_eq!(tapering_vec.len(), 14);
        assert_eq!(4.,tapering_vec[3]);
        assert_eq!(1.,tapering_vec[0]);
        assert_eq!(14.,tapering_vec[13]);
        tapering_vec.push(77.);
        assert_eq!(tapering_vec.len(), 15);
        assert_eq!(77.,tapering_vec[14]);
        tapering_vec.remove(0);
        assert_eq!(tapering_vec.len(), 14);
        assert_eq!(2.,tapering_vec[0]);
        tapering_vec.remove(0);
        assert_eq!(tapering_vec.len(), 13);
        assert_eq!(3.,tapering_vec[0]);
        assert_eq!(77.,tapering_vec[12]);
    }




}












