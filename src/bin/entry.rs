//! The entry point for interacting with the dydx, and phemex exchange.  Invoke the main by calling the program with an option - see the program. 
//!
//! Known shittiness: the all-markets call dies after about 12 hours.  The consumer probably could also write the data to mongo, and truncate the kafka partition post consumption.collection
//!
//! The basic thing we're doing here is analysis on dydx data, and enacting on it at Phemex, using perps (ideally).

mod dydx;
mod phemex;
mod config;
mod utils;

use std::collections::HashSet;

use time::Duration as NormalDuration;
use dydx::TLDYDXMarket;
use dydx::TLDYDXOrderbook;
use mongodb::{Client};

use time::Duration;
use std::{fs};
//use std::fs::File;
use std::path::{PathBuf};

use crate::config::Config;

use slurper::*;
use log::{info,debug,warn,error};
use std::error::Error;
//use std::convert::TryFrom;
use self::models::{ClusterBomb,ClusterBombTriple,ClusterBombTripleBonused,ThreeDimensionalExtract,FourDimensionalExtract};
use futures::future::join_all;
use chrono::{DateTime,Utc,SecondsFormat};
use tokio::time as TokioTime;  //renamed norm duration so could use this for interval
use tokio::time::Duration as TokioDuration;  //renamed norm duration so could use this for interval
//use kafka::error::Error as KafkaError;
use kafka::producer::{Producer, Record, RequiredAcks};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};


use hex::encode as hex_encode;
use hmac::{Hmac, Mac, NewMac};
use sha2::Sha256;


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
    info!("processing on directive input and optional gtedate: {} {}", matches.value_of("INPUT").unwrap(), matches.value_of("GTEDATE").unwrap_or("<NONE>"));

    // almost all use mongo, so declaring for all options
    let client = Client::with_uri_str(&Config::from_env().expect("Server configuration").local_mongo).await?;
    let database = client.database(&Config::from_env().expect("Server configuration").tldb);
    let dydxcol = database.collection::<TLDYDXMarket>(THE_TRADELLAMA_DYDX_SNAPSHOT_COLLECTION);            
    let dydxobcol = database.collection::<TLDYDXOrderbook>(THE_TRADELLAMA_DYDX_ORDERBOOK_COLLECTION);            

    let ms = utils::MongoSpecs {
        client: &client,
        database: &database,
    };

    let ks_dydx = utils::KafkaSpecs {
        broker: Config::from_env().expect("Server configuration").local_kafka_broker,
        topic: "dydx-markets".to_string()
    };

    let km_dydx = utils::KafkaMongo {
        k: ks_dydx.clone(),
        m: ms
    };

    let time_ranges = utils::get_time_ranges("2022-02-22 00:00:00","2022-02-28 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap();


    match matches.value_of("INPUT").unwrap() {

        // skipping the kafka pub sub, as 1 it's a pain, and 2, i don't think kafka can take big long arrays
        // will probably regret later
        "poc" => {

            for tr in time_ranges{
                info!("Operating on range: {} {}", &tr.gtedate, &tr.ltdate);
                let obshit = dydx::ClusterConfiguration {
                    gtedate: tr.gtedate, 
                    ltdate: tr.ltdate, 
                    snap_count: 180,
                };
                obshit.orderbook_spread_price_volatility(&dydxcol,&dydxobcol).await?
            }


        },

        "list-perpetuals-phemex" => {
            for (_symbol, product) in phemex::get_perpetuals().await.unwrap() {
                println!("{}", product);
            }
        },

        "list-all-products-phemex" => {
            for item in phemex::get_products().await.unwrap() {
                println!("{}", item);
            }
            for item in phemex::get_currencies().await.unwrap() {
                println!("{}", item);
            }

        },

        "account-phemex" => {
            phemex::PhemexAuth::default().get_balances().await?;
        },

        "select-markets-phemex" => phemex::poc2().await?,

        "clean-dydx" => {
            let tr = utils::TimeRange::annihilation();
            tr.delete_exact_range_tldydxmarket(&dydxcol).await?
        },

        "consume-dydx" => {

            let kmdydx = dydx::KMDYDX {
                km: km_dydx,
                dydxcol: database.collection::<TLDYDXMarket>(THE_TRADELLAMA_DYDX_SNAPSHOT_COLLECTION)
            };

            kmdydx.consume().await?;
        },
        
        "gap-analysis-dydx" => {
            let time_ranges = utils::get_time_ranges("2021-12-01 00:00:00","2022-01-01 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap();
            for otr in &time_ranges{
                let hourlies = otr.get_hourlies().unwrap();
                assert_eq!(hourlies.len(),24);
                let hcol: Vec<_> = (0..24).map(|n| hourlies[n].get_range_count(&dydxcol)).collect();
                let rvec = join_all(hcol).await;

                for (idx, r) in rvec.iter().enumerate() {
                    match r {
                        Ok(aggsv) => {
                            for aggs in aggsv {
                                debug!("{} {} {} {:?}", otr.gtedate, otr.ltdate, idx, aggs);
                            }
                        },
                        Err(error) => {
                            error!("Hour {:?} Error: {:?}", idx+1,  error);
                            panic!("We choose to no longer live.")                            
                        }
                    }
                }
            }
        },

        "iov-dydx" => {

            warn!("has become useless");

            // for tr in time_ranges{
            //     info!("Operating on range: {} {}", &tr.gtedate, &tr.ltdate);
            //     let iopv = dydx::ClusterConfiguration {
            //         gtedate: tr.gtedate, 
            //         ltdate: tr.ltdate, 
            //         snap_count: 180,
            //     };
            //     iopv.index_oracle_volatility(&dydxcol).await?
            // }
        },


        "iopv-dydx" => {
            for tr in time_ranges{
                info!("Operating on range: {} {}", &tr.gtedate, &tr.ltdate);
                let iopv = dydx::ClusterConfiguration {
                    gtedate: tr.gtedate, 
                    ltdate: tr.ltdate, 
                    snap_count: 180,
                };
                iopv.index_oracle_price_volatility(&dydxcol).await?
            }
        },


        "oipv-dydx" => {

            for tr in time_ranges{
                info!("Operating on range: {} {}", &tr.gtedate, &tr.ltdate);
                let iopv = dydx::ClusterConfiguration {
                    gtedate: tr.gtedate, 
                    ltdate: tr.ltdate, 
                    snap_count: 180,
                };
                iopv.open_interest_price_volatility(&dydxcol).await?
            }
        },

        "nfrpv-dydx" => {
            for tr in time_ranges{
                info!("Operating on range: {} {}", &tr.gtedate, &tr.ltdate);        
                let iopv = dydx::ClusterConfiguration {
                    gtedate: tr.gtedate, 
                    ltdate: tr.ltdate, 
                    snap_count: 180,
                };
                iopv.funding_rate_price_volatility(&dydxcol).await?
            }
        },

        "vdpv-dydx" => {
            for tr in time_ranges{
                info!("Operating on range: {} {}", &tr.gtedate, &tr.ltdate);        
                let iopv = dydx::ClusterConfiguration {
                    gtedate: tr.gtedate, 
                    ltdate: tr.ltdate, 
                    snap_count: 180,
                };
                iopv.volatility_delta_price_volatility(&dydxcol).await?
            }
        },


        "all-markets-dydx" => {

            let kdydx = dydx::KDYDX {
                k: ks_dydx.clone(),
                lookback: 600,
                frequency: 1000,
                cap: 10000000,
            };

            kdydx.process_all_markets().await?;
        },


        "orderbooks-dydx" => {

            info!("remember, straight to mongo this one");
            info!("this process should be daemonized");
            info!("adding two zeros to the await interval compared to the market snapshots");
            let mut interval = TokioTime::interval(TokioDuration::from_millis(100000));

            let markets = dydx::get_markets().await.unwrap().into_iter();

            let cap = 10000000;
            let mut tmpcnt = 0;
            loop {
                if tmpcnt == cap {
                    break;
                } else {
                    tmpcnt+=1;
                }

                let dcol: Vec<_> = markets.clone().map(|item| dydx::get_orderbook(item.market)).collect();
                let rvec = join_all(dcol).await;
                debug!("{:?}", rvec);

                for (idx, r) in rvec.iter().enumerate() {
                    match r {
                        Ok(tldo) => {
                            debug!("inserting {:?}", tldo);
                            let _result = database.collection::<TLDYDXOrderbook>(THE_TRADELLAMA_DYDX_ORDERBOOK_COLLECTION).insert_one(tldo, None).await?;                                                                                                    
                        },
                        Err(error) => {
                            error!("Err index {:?} Error: {:?}", idx+1,  error);
                            panic!("We choose to no longer live.")                            
                        }
                    }
                }
                interval.tick().await; 
            }

        },




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












