// this use statement gets you access to the lib file
use slurper::*;


use log::{info,debug};
use std::error::Error;
//use std::convert::TryFrom;
use self::models::{DYDXMarkets,DYDXMarket};
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
            for item in get_markets().await.unwrap() {
                println!("{}", item.get_tl_version().unwrap());
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












