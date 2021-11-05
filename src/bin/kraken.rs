// this use statement gets you access to the lib file
use slurper::*;

use log::{info,debug,error};
use std::error::Error;
//use std::convert::TryFrom;
use self::models::{SourceThingLastUpdate,AssetPair,KrakenAssetPairs,KrakenAssetPair,KrakenAssets, KrakenAsset, KafkaKrakenTrade};
use chrono::{DateTime,NaiveDateTime,Utc,SecondsFormat};

//use futures::stream::TryStreamExt;
use mongodb::{Client};
//use mongodb::{bson::doc};
//use mongodb::options::{FindOptions};


use std::collections::HashMap;

use std::time::Duration;

use kafka::producer::{Producer, Record, RequiredAcks};

use futures::future::join_all;


use std::str;

#[macro_use]
extern crate clap;
use clap::App;

extern crate serde;
extern crate base64;


use serde_json::{Value};

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

    for (_k,v) in payload.asset_pairs {
        rvec.push(v);                    
    }

    Ok(rvec)
} 

async fn get_assets() -> Result<Vec<KrakenAsset>, Box<dyn Error>> {

     let mut rvec = Vec::new();

    let request_url = format!("{}", ASSETS_URL);
    let response = reqwest::get(&request_url).await?;

    let payload: KrakenAssets = response.json().await?;

    for (_k,v) in payload.assets {
        rvec.push(v);                    
    }

    Ok(rvec)
} 

// I could not get this to parse using serde the usual way - see model KrakenTrade
// so different than normal reqwest map to struct
async fn process_trades(item: KrakenAssetPair) -> Result<i64, Box<dyn Error>> {

    let mut max_market_date_hm = HashMap::new();

    let altname = &item.altname;
    let recreated_name = format!("{}{}", item.base, item.quote);
    let ts_url1 = format!("{}?pair={}", TRADES_URL, item.altname); // query trades with altname, works - but some need the recreated name in the return json 

    let broker = "localhost:9092";
    let topic = "kraken-markets";

    let client = Client::with_uri_str(LOCAL_MONGO).await?;
    let database = client.database(THE_DATABASE);
    let collection = database.collection::<SourceThingLastUpdate>(THE_SOURCE_THING_LAST_UPDATE_COLLECTION);


    let mut producer = Producer::from_hosts(vec![broker.to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()?;


        let asset_pair = AssetPair {
            altname: altname,
            base_asset: &item.base,
            quote_asset: &item.quote,
        };

    let big_bang = asset_pair.get_last_updated_trade("kraken", &collection).await?; // you could try augment the url to expand based on this distance
//    let gtedate = big_bang.to_rfc3339_opts(SecondsFormat::Secs, true);


    let request_url_ts = format!("{}", ts_url1);
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

            let mut dropped_trades: i64 = 0; 

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

                if trade_date > big_bang { // this is redundant I think to the above gtedate

                    let new_kafka_kraken_trade = KafkaKrakenTrade {
                        asset_pair: &item.altname,
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
                   max_market_date_hm.entry(altname.clone()).or_insert(trade_date.clone());

                } else {
                    dropped_trades -= 1;
                }

            };

            for (k,v) in max_market_date_hm {
                let trade_date = &v;
                let new_cm = SourceThingLastUpdate {
                    last_known_trade_date: trade_date.with_timezone(&Utc),
                    source: "kraken".to_string(),
                    thing: k,
                    thing_description: "asset pair".to_string()
                };
                println!("{}", new_cm);
                let _result = collection.insert_one(new_cm, None).await?;                                    
            }


            debug!("Processed {} trades, though we dropped {} trades, for a total send to kafka of {} trades.", trades.len(), dropped_trades, trades.len() as i64 - dropped_trades);
            Ok(trades.len() as i64 - dropped_trades)


        },
        _ => {
            Ok(0)
        }
    }


}


#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {

    env_logger::init(); 


    let yaml = load_yaml!("../cmds.yml");
    let matches = App::from_yaml(yaml).get_matches();
    info!("processing on directive input: {}", matches.value_of("INPUT").unwrap());

    match matches.value_of("INPUT").unwrap() {

        "all-asset-pairs" => {

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

            let dcol: Vec<_> = get_asset_pairs().await.unwrap().into_iter().map(|item| process_trades(item)).collect();
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

    #[test]
    fn it_works_test_file_exists() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn it_works_can_open_close_file() {
        assert_eq!(3 + 3, 6);
    }


}












