// this use statement gets you access to the lib file
use slurper::*;


use log::{info,debug};
use std::error::Error;
//use std::convert::TryFrom;
use self::models::{DYDXMarkets,DYDXMarket,TLDYDXMarket};
use chrono::{Utc,SecondsFormat};
use time::Duration as NormalDuration;
use tokio::time as TokioTime;  //renamed norm duration so could use this for interval
use tokio::time::Duration as TokioDuration;  //renamed norm duration so could use this for interval

//use futures::stream::TryStreamExt;
use mongodb::{Client};
use mongodb::{bson::doc};
//use mongodb::options::{FindOptions};

use std::collections::HashMap;

use std::time::Duration;

//use kafka::error::Error as KafkaError;
use kafka::producer::{Producer, Record, RequiredAcks};
//use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};


#[macro_use]
extern crate clap;
use clap::App;

extern crate serde;
extern crate base64;



use hex::encode as hex_encode;
use hmac::{Hmac, Mac, NewMac};
use sha2::Sha256;


use serde::{Deserialize, Serialize};


const LOOKBACK_OPEN_INTEREST: i64 = 100000000; // 1666 minutes or 27 ish hours

// i know bad but will change, only works on my IP, and there's 20 bucks in the account
// const API_TOKEN: &str = "89124f02-5e64-436a-bb3c-a5f4d720664d";
// const API_SECRET: &str = "9B1Yl3NZV0DJS7Q3xJ9LRgfJEFwwdhST0Ihh0DBjePo5Y2I1MjNmZS1hOTkzLTQzNjMtODQ3MS03ZDY0N2M1ZTZmOTk";


const MARKETS_URL: &str = "https://api.dydx.exchange/v3/markets";


// const PRODUCTS_URL: &str = "https://api.phemex.com/public/products";
// const ACCOUNT_POSTIONS_URL: &str = "https://vapi.phemex.com/accounts/accountPositions?currency=USD";
// const ACCOUNT_POSTIONS_MSG: &str = "/accounts/accountPositionscurrency=USD";

// const PLACE_ORDER_URL: &str = "https://vapi.phemex.com/orders";
// const PLACE_ORDER_MSG: &str = "/orders";


// async fn get_perpetuals() -> Result<HashMap<String, DYDXMarket>, Box<dyn Error>> {

//     let mut rmap = HashMap::new();

//     let request_url = format!("{}", PRODUCTS_URL);
//     let response = reqwest::get(&request_url).await?;

//     let payload: PhemexDataWrapperProducts = response.json().await?;
//     for item in payload.data.products {
//         if item.product_type == "Perpetual" {
//             rmap.entry(item.symbol.clone()).or_insert(item);
//         }
//     }
//     Ok(rmap)

// } 


async fn get_markets() -> Result<Vec<DYDXMarket>, Box<dyn Error>> {

     let mut rvec = Vec::new();

    let request_url = format!("{}", MARKETS_URL);
    let response = reqwest::get(&request_url).await?;

    debug!("here");
    let payload: DYDXMarkets = response.json().await?;
    debug!("and {:?}", payload);

    for (k,v) in payload.markets {
        debug!("{:?}",v);
        rvec.push(v);                    
    }

    Ok(rvec)
} 

// async fn get_currencies() -> Result<Vec<PhemexCurrency>, Box<dyn Error>> {

//      let mut rvec = Vec::new();

//     let request_url = format!("{}", PRODUCTS_URL);
//     let response = reqwest::get(&request_url).await?;

//     let payload: PhemexDataWrapperProducts = response.json().await?;
//     for item in payload.data.currencies {
//         rvec.push(item);
//     }

//     Ok(rvec)
// } 

// async fn get_market_data<'a>(symbol: &'a str) -> Result<PhemexMD, Box<dyn Error>> {

//     let request_url = format!("{}?symbol={}", MD_URL,symbol);
//     debug!("request_url {}", request_url);
//     let response = reqwest::get(&request_url).await?;

//     let payload: PhemexDataWrapperMD = response.json().await?;
//     Ok(payload.result)

// } 



#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {

    env_logger::init(); 


    let yaml = load_yaml!("../coinmetrics.yml");
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

        "all-markets" => {

            let broker = "localhost:9092";
            let topic = "dydx-markets";

            let mut producer = Producer::from_hosts(vec![broker.to_owned()])
                .with_ack_timeout(Duration::from_secs(1))
                .with_required_acks(RequiredAcks::One)
                .create()?;


            info!("this process should be daemonized");
            let mut interval = TokioTime::interval(TokioDuration::from_millis(1000));

            let mut tmpcnt = 0;
            loop {
                if tmpcnt == 1000000 {
                    break;
                } else {
                    tmpcnt+=1;
                }

                for item in get_markets().await.unwrap() {

                    let index_price = item.index_price.parse::<f64>().unwrap();
                    let oracle_price = item.oracle_price.parse::<f64>().unwrap();
                    let tl_derived_index_oracle_spread = (index_price - oracle_price) / oracle_price;
                    let price_change_24h = item.price_change_24h.parse::<f64>().unwrap();
                    let next_funding_rate = item.next_funding_rate.parse::<f64>().unwrap();
                    let min_order_size = item.min_order_size.parse::<f64>().unwrap();

                    let initial_margin_fraction = item.initial_margin_fraction.parse::<f64>().unwrap();
                    let maintenance_margin_fraction = item.maintenance_margin_fraction.parse::<f64>().unwrap();
                    let baseline_position_size = item.baseline_position_size.parse::<f64>().unwrap();
                    let incremental_position_size = item.incremental_position_size.parse::<f64>().unwrap();
                    let incremental_initial_margin_fraction = item.incremental_initial_margin_fraction.parse::<f64>().unwrap();
                    let volume_24h = item.volume_24h.parse::<f64>().unwrap();
                    let trades_24h = item.trades_24h.parse::<f64>().unwrap();
                    let open_interest = item.open_interest.parse::<f64>().unwrap();
                    let max_position_size = item.max_position_size.parse::<f64>().unwrap();
                    let asset_resolution = item.asset_resolution.parse::<f64>().unwrap();

                    let tlm = TLDYDXMarket {
                        snapshot_date: &Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                        market: &item.market,
                        status: &item.status,
                        base_asset: &item.base_asset,
                        quote_asset: &item.quote_asset,
                        step_size: item.step_size.parse::<f64>().unwrap(),
                        tick_size: item.tick_size.parse::<f64>().unwrap(),
                        index_price: index_price,
                        oracle_price: oracle_price,
                        tl_derived_index_oracle_spread: tl_derived_index_oracle_spread,
                        price_change_24h: price_change_24h,
                        next_funding_rate: next_funding_rate,
                        next_funding_at: &item.next_funding_at,
                        min_order_size: min_order_size,
                        instrument_type: &item.instrument_type,
                        initial_margin_fraction: initial_margin_fraction,
                        maintenance_margin_fraction: maintenance_margin_fraction,
                        baseline_position_size: baseline_position_size,
                        incremental_position_size: incremental_position_size,
                        incremental_initial_margin_fraction: incremental_initial_margin_fraction,
                        volume_24h: volume_24h,
                        trades_24h: trades_24h,
                        open_interest: open_interest,
                        max_position_size: max_position_size,
                        asset_resolution: asset_resolution,
                    };

                    println!("{}", tlm);
                    let data = serde_json::to_string(&tlm).expect("json serialization failed");
                    let data_as_bytes = data.as_bytes();

                    producer.send(&Record {
                        topic,
                        partition: -1,
                        key: (),
                        value: data_as_bytes,
                    })?;
                }
                interval.tick().await; 
            }

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












