//! The entry point for interacting with the dydx exchange.  Invoke the main by calling the program with one of two options: 
//!
//! RUST_LOG=DEBUG cargo run --bin dydx-entry all-markets
//!
//! RUST_LOG=DEBUG cargo run --bin dydx-entry consumer-mongo
//!
//! RUST_LOG=DEBUG cargo run --bin dydx-entry index-oracle-vol
//!
//! RUST_LOG=DEBUG cargo run --bin dydx-entry clean-dydx
//!
//! Known shittiness: the all-markets call dies after about 12 hours.  The consumer probably could also write the data to mongo, and truncate the kafka partition post consumption.collection

use dydx::TLDYDXMarket;
use mongodb::{Client};

use time::Duration;

mod dydx;
mod config;

use crate::config::Config;

use slurper::*;
use log::{info,debug,warn,error};
use std::error::Error;
//use std::convert::TryFrom;
use self::models::{ClusterBomb,ClusterBombTriple};
use chrono::{DateTime,Utc,SecondsFormat};
use tokio::time as TokioTime;  //renamed norm duration so could use this for interval
use tokio::time::Duration as TokioDuration;  //renamed norm duration so could use this for interval
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
    // let config = Config::from_env().expect("Server configuration");
    // debug!("local mongo is: {}", config.local_mongo);


    let yaml = load_yaml!("../cmds.yml");
    let matches = App::from_yaml(yaml).get_matches();
    info!("processing on directive input: {}", matches.value_of("INPUT").unwrap());

    match matches.value_of("INPUT").unwrap() {
        "clean-dydx" => dydx::delete_dydx_data_in_mongo().await?,            
        "consumer-mongo" => dydx::consume_dydx_topic().await?,
        "index-oracle-vol" => {

            let client = Client::with_uri_str(&Config::from_env().expect("Server configuration").local_mongo).await?;
            let database = client.database(&Config::from_env().expect("Server configuration").tldb);
            let dydxcol = database.collection::<TLDYDXMarket>(THE_TRADELLAMA_DYDX_SNAPSHOT_COLLECTION);            
            let iopv = dydx::IOVolPerf {
                gtedate: Utc::now() - Duration::milliseconds(30000000), // 500 minutes
                snap_count: 180,
            };
            iopv.index_oracle_volatility("/tmp/cluster_bomb.csv","/tmp/cluster_bomb_triple",&dydxcol).await?
        },
        "all-markets" => dydx::process_all_markets().await?,
        _ => error!("Unrecognized input parm."),
    }

    Ok(())

}


#[cfg(test)]
mod tests {

    use mongodb::Client;
    use super::*;

    #[test]
    fn it_works_test_file_exists() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn it_works_can_open_close_file() {
        assert_eq!(3 + 3, 6);
    }

    #[tokio::test]
    async fn dydx_still_has_markets() {
        let mkts = dydx::get_markets().await.expect("holy sheep shit");
        assert_ne!(0,mkts.len())
    }

    #[tokio::test]
    async fn dydx_has_been_processed_at_least_once() {
        let client = Client::with_uri_str(&Config::from_env().expect("Server configuration").local_mongo).await.expect("holier sheep shit");
        let database = client.database(&Config::from_env().expect("Server configuration").tldb);
        let dydxcol = database.collection::<dydx::TLDYDXMarket>(THE_TRADELLAMA_DYDX_SNAPSHOT_COLLECTION);
        let enum_tldms = dydx::get_first_snapshot("FIL-USD").await.expect("holy sheep shit");

        let last_one_processed = enum_tldms.as_ref().unwrap().get_last_migrated_from_kafka(&dydxcol).await.expect("and even more holier");

        let mongo_snapshot_date = match enum_tldms.unwrap() {
            dydx::DYDXM::TLDYDXMarket(t) => t.mongo_snapshot_date, 
            dydx::DYDXM::DYDXMarket(_) => Utc::now(), 
        };

        assert_ne!(mongo_snapshot_date.timestamp(), last_one_processed.timestamp());

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












