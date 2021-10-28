// this use statement gets you access to the lib file
use slurper::*;

use log::{info,debug,error};
use std::error::Error;
//use std::convert::TryFrom;
use self::models::{KrakenAssetPairs,KrakenAssetPair,KrakenAssets, KrakenAsset, KafkaKrakenTrade, TLDYDXMarket};
use chrono::{DateTime,NaiveDateTime,Utc,SecondsFormat};
use time::Duration as NormalDuration;
use tokio::time as TokioTime;  //renamed norm duration so could use this for interval
use tokio::time::Duration as TokioDuration;  //renamed norm duration so could use this for interval

//use futures::stream::TryStreamExt;
use mongodb::{Client};
use mongodb::{bson::doc};
//use mongodb::options::{FindOptions};

use std::collections::HashMap;

use std::time::Duration;

use kafka::error::Error as KafkaError;
use kafka::producer::{Producer, Record, RequiredAcks};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

use std::str;

#[macro_use]
extern crate clap;
use clap::App;

extern crate serde;
extern crate base64;

use hex::encode as hex_encode;
use hmac::{Hmac, Mac, NewMac};
use sha2::Sha256;


use serde::{Deserialize, Serialize};
use serde_json::{Value};

const LOOKBACK_OPEN_INTEREST: i64 = 100000000; // 1666 minutes or 27 ish hours

// i know bad but will change, only works on my IP, and there's 20 bucks in the account
// const API_TOKEN: &str = "89124f02-5e64-436a-bb3c-a5f4d720664d";
// const API_SECRET: &str = "9B1Yl3NZV0DJS7Q3xJ9LRgfJEFwwdhST0Ihh0DBjePo5Y2I1MjNmZS1hOTkzLTQzNjMtODQ3MS03ZDY0N2M1ZTZmOTk";


const ASSET_PAIRS_URL: &str = "https://api.kraken.com/0/public/AssetPairs";
const ASSETS_URL: &str = "https://api.kraken.com/0/public/Assets";
const TRADES_URL: &str = "https://api.kraken.com/0/public/Trades";


async fn get_asset_pairs() -> Result<Vec<KrakenAssetPair>, Box<dyn Error>> {

     let mut rvec = Vec::new();

    let request_url = format!("{}", ASSET_PAIRS_URL);
    let response = reqwest::get(&request_url).await?;

    let payload: KrakenAssetPairs = response.json().await?;

    for (k,v) in payload.asset_pairs {
        rvec.push(v);                    
    }

    Ok(rvec)
} 

async fn get_assets() -> Result<Vec<KrakenAsset>, Box<dyn Error>> {

     let mut rvec = Vec::new();

    let request_url = format!("{}", ASSETS_URL);
    let response = reqwest::get(&request_url).await?;

    let payload: KrakenAssets = response.json().await?;

    for (k,v) in payload.assets {
        rvec.push(v);                    
    }

    Ok(rvec)
} 

// I could not get this to parse using serde the usual way - see model KrakenTrade
// so different than normal reqwest map to struct
async fn process_trades<'a>(item: &'a KrakenAssetPair) -> Result<usize, Box<dyn Error>> {

    let recreated_name = format!("{}{}", item.base, item.quote);
    let ts_url1 = format!("{}?pair={}", TRADES_URL, item.altname); // query trades with altname, works - but some need the recreated name in the return json 

    let broker = "localhost:9092";
    let topic = "kraken-markets";

    let mut producer = Producer::from_hosts(vec![broker.to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()?;

    let request_url_ts = format!("{}", whole_url);
    let response_ts = reqwest::get(&request_url_ts).await?;
    let text_representation_of_json = response_ts.text().await?;
    let json_from_text: Value = serde_json::from_str(&text_representation_of_json)?;

    let trades_array = match json_from_text["result"][altname].as_array() {
        None => {
            info!("This pair does not return using altname, I will try recreated {} {}", altname, recreated_name);
            match json_from_text["result"][altname].as_array() {
                None => {
                    error!("Neither the altname nor recreated name are in this array: {}", json_from_text);
                    error!("I am going to return an empty vessel");
                    None
                },
                _ => {
                    Some(json_from_text["result"][recreated_name].as_array().unwrap())
                }
            }
        },
        _ => {
            Some(json_from_text["result"][altname].as_array().unwrap())
        }
    };


    match trades_array {
        Some(trades) => {

            for trade in trades {
                debug!("{}", trade);

                let tx_type = match trade[3].as_str().unwrap() {
                    "b" => "Buy",
                    "s" => "Sell",
                    _ => trade[3].as_str().unwrap()
                };

                let order_type = match trade[4].as_str().unwrap() {
                    "m" => "Market",
                    "l" => "Limit",
                    _ => trade[4].as_str().unwrap()
                };

                let some_other_thing = match trade[5].as_str().unwrap() {
                    "" => None,
                   _ => Some(trade[5].as_str().unwrap())
                };

                let unix_epoch = trade[2].as_f64().unwrap();
                let unix_epoch_as_int = unix_epoch as i64;


                let trade_date = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(unix_epoch_as_int, 0), Utc);
                let trade_date_str = trade_date.to_rfc3339_opts(SecondsFormat::Secs, true);
                debug!("{} {} {} {}", unix_epoch, unix_epoch_as_int, trade_date, trade_date_str);

                let new_kafka_kraken_trade = KafkaKrakenTrade {
                    price: trade[0].as_str().unwrap().parse::<f64>().unwrap(),
                    quantity: trade[1].as_str().unwrap().parse::<f64>().unwrap(),
                    trade_date: &trade_date_str, 
                    tx_type: tx_type,
                    order_type: order_type,
                    some_other_thing: some_other_thing
                };

                println!("{}", new_kafka_kraken_trade);

                let data = serde_json::to_string(&new_kafka_kraken_trade).expect("json serialization failed");
                let data_as_bytes = data.as_bytes();

                producer.send(&Record {
                    topic,
                    partition: -1,
                    key: (),
                    value: data_as_bytes,
                })?;


            };

            Ok(trades.len())



        },
        _ => {
            Ok(0)
        }
    }


}


#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {

    env_logger::init(); 


    let yaml = load_yaml!("../coinmetrics.yml");
    let matches = App::from_yaml(yaml).get_matches();
    info!("processing on directive input: {}", matches.value_of("INPUT").unwrap());

    match matches.value_of("INPUT").unwrap() {

        "all-asset-pairs" => {

            let broker = "localhost:9092";
            let topic = "kraken-markets";

            let mut producer = Producer::from_hosts(vec![broker.to_owned()])
                .with_ack_timeout(Duration::from_secs(1))
                .with_required_acks(RequiredAcks::One)
                .create()?;

            for item in get_asset_pairs().await.unwrap() {

                println!("{}", item);
            }

        },
        "all-assets" => {

            for item in get_assets().await.unwrap() {
                println!("{}", item);
            }

        },

        "trades" => {

            let broker = "localhost:9092";
            let topic = "kraken-markets";

            let mut producer = Producer::from_hosts(vec![broker.to_owned()])
                .with_ack_timeout(Duration::from_secs(1))
                .with_required_acks(RequiredAcks::One)
                .create()?;

            let dcol: Vec<_> = get_asset_pairs().await.unwrap().into_iter().map(|item| process_trades(&item)).collect();
            let _rdvec = join_all(dcol).await;

        },


        _ => {
            debug!("Unrecognized input parm.");
        }


    }



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












