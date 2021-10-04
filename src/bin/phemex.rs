// this use statement gets you access to the lib file
use slurper::*;


use log::{info,debug};
use std::error::Error;
//use std::convert::TryFrom;
use self::models::{AnalysisArtifact,PhemexDataWrapperAccount,PhemexDataWrapperProducts, PhemexProduct, PhemexCurrency,PhemexDataWrapperMD, PhemexMD,TLPhemexMDSnapshot, CryptoLiquidation, Trades};
use chrono::{DateTime,Utc};
use time::Duration as NormalDuration;
use tokio::time as TokioTime;  //renamed norm duration so could use this for interval
use tokio::time::Duration as TokioDuration;  //renamed norm duration so could use this for interval

use futures::stream::TryStreamExt;
use mongodb::{Client};
use mongodb::{bson::doc};
use mongodb::options::{FindOptions};

use std::collections::HashMap;



#[macro_use]
extern crate clap;
use clap::App;

extern crate serde;
extern crate base64;



use hex::encode as hex_encode;
use hmac::{Hmac, Mac, NewMac};
use sha2::Sha256;


use serde::{Deserialize, Serialize};

// Create alias for HMAC-SHA256
//type HmacSha256 = Hmac<Sha256>;

const LOOKBACK_OPEN_INTEREST: i64 = 100000000; // 1666 minutes or 27 ish hours

// i know bad but will change, only works on my IP, and there's 20 bucks in the account
const API_TOKEN: &str = "89124f02-5e64-436a-bb3c-a5f4d720664d";
const API_SECRET: &str = "9B1Yl3NZV0DJS7Q3xJ9LRgfJEFwwdhST0Ihh0DBjePo5Y2I1MjNmZS1hOTkzLTQzNjMtODQ3MS03ZDY0N2M1ZTZmOTk";


const MD_URL: &str = "https://api.phemex.com/md/ticker/24hr";

const PRODUCTS_URL: &str = "https://api.phemex.com/public/products";
const ACCOUNT_POSTIONS_URL: &str = "https://vapi.phemex.com/accounts/accountPositions?currency=USD";
const ACCOUNT_POSTIONS_MSG: &str = "/accounts/accountPositionscurrency=USD";

const PLACE_ORDER_URL: &str = "https://vapi.phemex.com/orders";
const PLACE_ORDER_MSG: &str = "/orders";



#[derive(Serialize, Deserialize)]
struct Order {
    #[serde(rename = "actionBy", default)]
    action_by: String,
    #[serde(rename = "symbol", default)]
    symbol: String,
    #[serde(rename = "clOrdId", default)]
    client_order_id: String,
    #[serde(rename = "side", default)]
    side: String,
    #[serde(rename = "priceEp", default)]
    price_ep: i64,
    #[serde(rename = "orderQty", default)]
    quantity: f64,
    #[serde(rename = "orderType", default)]
    order_type: String,
}




async fn get_perpetuals() -> Result<HashMap<String, PhemexProduct>, Box<dyn Error>> {

    let mut rmap = HashMap::new();

    let request_url = format!("{}", PRODUCTS_URL);
    let response = reqwest::get(&request_url).await?;

    let payload: PhemexDataWrapperProducts = response.json().await?;
    for item in payload.data.products {
        if item.product_type == "Perpetual" {
            rmap.entry(item.symbol.clone()).or_insert(item);
        }
    }
    Ok(rmap)

} 


async fn get_products() -> Result<Vec<PhemexProduct>, Box<dyn Error>> {

     let mut rvec = Vec::new();

    let request_url = format!("{}", PRODUCTS_URL);
    let response = reqwest::get(&request_url).await?;

    let payload: PhemexDataWrapperProducts = response.json().await?;
    for item in payload.data.products {
        rvec.push(item);
    }

    Ok(rvec)
} 

async fn get_currencies() -> Result<Vec<PhemexCurrency>, Box<dyn Error>> {

     let mut rvec = Vec::new();

    let request_url = format!("{}", PRODUCTS_URL);
    let response = reqwest::get(&request_url).await?;

    let payload: PhemexDataWrapperProducts = response.json().await?;
    for item in payload.data.currencies {
        rvec.push(item);
    }

    Ok(rvec)
} 

async fn get_market_data<'a>(symbol: &'a str) -> Result<PhemexMD, Box<dyn Error>> {

    let request_url = format!("{}?symbol={}", MD_URL,symbol);
    debug!("request_url {}", request_url);
    let response = reqwest::get(&request_url).await?;

    let payload: PhemexDataWrapperMD = response.json().await?;
    Ok(payload.result)

} 



#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {

    env_logger::init(); 


    let yaml = load_yaml!("../coinmetrics.yml");
    let matches = App::from_yaml(yaml).get_matches();
//    debug!("{:?}",matches);

    let client = Client::with_uri_str(LOCAL_MONGO).await?;
    let database = client.database(THE_DATABASE);
    let tlphsnapcollection = database.collection::<TLPhemexMDSnapshot>(THE_TRADELLAMA_PHEMEX_MD_SNAPSHOT_COLLECTION);
    let lcollection = database.collection::<CryptoLiquidation>(THE_CRYPTO_LIQUIDATION_COLLECTION);

    // Calling .unwrap() is safe here because "INPUT" is required (if "INPUT" wasn't
    // required we could have used an 'if let' to conditionally get the value)
    info!("processing on directive input: {}", matches.value_of("INPUT").unwrap());

    match matches.value_of("INPUT").unwrap() {

        "all-products" => {
            for item in get_products().await.unwrap() {
                println!("{}", item);
            }
            for item in get_currencies().await.unwrap() {
                println!("{}", item);
            }
        },

        "just-perpetuals" => {
            for (_symbol, product) in get_perpetuals().await.unwrap() {
                println!("{}", product);
            }
        },


        "push-select-md" => {

            let the_eth_boys = vec!["ETHUSD"];
            info!("this process should be daemonized");
            let mut interval = TokioTime::interval(TokioDuration::from_millis(5000));
            loop {
                for eth in &the_eth_boys {
                    let current_md = get_market_data(eth).await?;
                    let new_tlphsnap = TLPhemexMDSnapshot {
                        snapshot_date: Utc::now(),
                        symbol: eth.to_string(),
                        open: current_md.open,
                        high: current_md.high,
                        low: current_md.low,
                        close: current_md.close,
                        index_price: current_md.index_price,
                        mark_price: current_md.mark_price,
                        open_interest: current_md.open_interest
                    };
                    println!("{}", new_tlphsnap);
                    let _result = tlphsnapcollection.insert_one(&new_tlphsnap, None).await?;                                                                                
                    interval.tick().await; 
                }
            }

        },


        "walk-ethusd-open-interest" => {
            
            let ltdate = Utc::now();
            let aa = AnalysisArtifact {
                ltdate: ltdate,
                symbol: Some("ETHUSD".to_string())
            };

            let history = aa.get_history(&LOOKBACK_OPEN_INTEREST,&tlphsnapcollection).await?;
            let disasf_prices = delta_walk_integers(&history.iter().map(|n| n.mark_price).collect::<Vec<i64>>());
            let disasf_open_interest = delta_walk_integers(&history.iter().map(|n| n.open_interest).collect::<Vec<i64>>());
            let its_snapshots = interval_walk_timestamps(&history.iter().map(|n| n.snapshot_date).collect::<Vec<DateTime<Utc>>>());

            for (idx, q) in history.iter().enumerate() {
                println!("{} {:>9.4} {:>9.4} {}", q, disasf_prices[idx], disasf_open_interest[idx], its_snapshots[idx]);
            }

            debug!("open interest delta {:?}", &aa.get_open_interest_delta(&LOOKBACK_OPEN_INTEREST, &tlphsnapcollection).await?);
            debug!("mark price delta {:?}", &aa.get_mark_price_delta(&LOOKBACK_OPEN_INTEREST, &tlphsnapcollection).await?);


        },

        "account-positions" => {

            info!("this queries all positions in account");
            let exp = (Utc::now() + NormalDuration::milliseconds(10000)).timestamp();
            let expstr = &exp.to_string();            

            let request_url = format!("{}",ACCOUNT_POSTIONS_URL);
            let msg = format!("{}{}", ACCOUNT_POSTIONS_MSG,&expstr);

            let mut signed_key = Hmac::<Sha256>::new_from_slice(API_SECRET.as_bytes()).unwrap();
            signed_key.update(msg.as_bytes());
            let signature = hex_encode(signed_key.finalize().into_bytes());            

            let client = reqwest::Client::builder().build()?;
            let response = client.get(&request_url).header("x-phemex-access-token", API_TOKEN).header("x-phemex-request-expiry", exp).header("x-phemex-request-signature", signature).send().await?;

            let payload: PhemexDataWrapperAccount = response.json().await?;
            for p in payload.data.positions {
                println!("{}", p);           
                let perps = get_perpetuals().await;
                let contract_size = perps.unwrap()[&p.symbol].contract_size;
                let current_md = get_market_data(&p.symbol).await?;
                let current_market_price = current_md.mark_price as f64 / 10000.00;
                debug!("current market price is: {}", current_market_price);
//                let pos = (p.size as f64 * contract_size) * p.mark_price;
                let pos = (p.size as f64 * contract_size) * current_market_price;
                let upl = if p.side == "Buy" {
                    ((p.size as f64 * contract_size) * current_market_price) - ((p.size as f64 * contract_size) * p.avg_entry_price)
                } else {
                    ((p.size as f64 * contract_size) * p.avg_entry_price) - ((p.size as f64 * contract_size) * current_market_price)
                };

// inverse?                    ((p.size as f64 / contract_size) / p.avg_entry_price) - ((p.size as f64 * contract_size) / current_market_price)                    

                debug!("{:?} {:?} {:?}      the open interest is {:?}", pos, upl, pos+upl, current_md.open_interest);

                let new_tlphsnap = TLPhemexMDSnapshot {
                    snapshot_date: Utc::now(),
                    symbol: p.symbol,
                    open: current_md.open,
                    high: current_md.high,
                    low: current_md.low,
                    close: current_md.close,
                    index_price: current_md.index_price,
                    mark_price: current_md.mark_price,
                    open_interest: current_md.open_interest
                };

                println!("{}", new_tlphsnap);
                let _result = tlphsnapcollection.insert_one(&new_tlphsnap, None).await?;                                                                
                let tlphsnhist = new_tlphsnap.get_history(&LOOKBACK_OPEN_INTEREST,&tlphsnapcollection).await?;

                let disasf_prices = delta_walk_integers(&tlphsnhist.iter().map(|n| n.mark_price).collect::<Vec<i64>>());
                let disasf_open_interest = delta_walk_integers(&tlphsnhist.iter().map(|n| n.open_interest).collect::<Vec<i64>>());
                let its_snapshots = interval_walk_timestamps(&tlphsnhist.iter().map(|n| n.snapshot_date).collect::<Vec<DateTime<Utc>>>());

                for (idx, q) in tlphsnhist.iter().enumerate() {
                    println!("{} {:>9.4} {:>9.4} {}", q, disasf_prices[idx], disasf_open_interest[idx], its_snapshots[idx]);
                }

                debug!("open interest delta {:?}", &new_tlphsnap.get_open_interest_delta(&LOOKBACK_OPEN_INTEREST, &tlphsnapcollection).await?);
                debug!("mark price delta {:?}", &new_tlphsnap.get_mark_price_delta(&LOOKBACK_OPEN_INTEREST, &tlphsnapcollection).await?);

                let lh = &new_tlphsnap.get_liquidations(&LOOKBACK_OPEN_INTEREST, &lcollection).await?;
                let new_trades = Trades {
                    vts: lh.clone()
                };

                debug!("total volume of liquidations for the last 26 hours is: {}", new_trades.get_total_volume().unwrap());
                let (p,q,pq,rkm) = new_trades.get_pqkm().unwrap();

                for (idx, km) in rkm.iter().enumerate() {
                    debug!("{:?} {:?} {:?}", p[idx],q[idx],km);
                }


            }


        },


        "place-order" => {

            info!("currently hardcoded");
            let exp = (Utc::now() + NormalDuration::milliseconds(10000)).timestamp();
            let expstr = &exp.to_string();            

            let order = Order {
                action_by: "FromOrderPlacement".to_string(),
                symbol: "ETHUSD".to_string(),
                client_order_id: "".to_string(),
                side: "Sell".to_string(),
                price_ep: 32600000,
                quantity: 5.0,
                order_type: "Limit".to_string()
            };

            let params = serde_json::to_string(&order)?;

            debug!("{}", params);

            let request_url = format!("{}",PLACE_ORDER_URL);
//            let request_url = format!("{}","https://httpbin.org/post");


            let msg = format!("{}{}{}", PLACE_ORDER_MSG,&expstr,params);
            debug!("{}",msg);

            let mut signed_key = Hmac::<Sha256>::new_from_slice(API_SECRET.as_bytes()).unwrap();
            signed_key.update(msg.as_bytes());
            let signature = hex_encode(signed_key.finalize().into_bytes());            

            let client = reqwest::Client::builder().build()?;
            let response = client.post(&request_url).header("x-phemex-access-token", API_TOKEN).header("x-phemex-request-expiry", exp).header("x-phemex-request-signature", signature).json(&params).body(params.to_owned()).send().await?;

            let payload = response.text().await?;
            debug!("{:?}", payload);


            // let payload: PhemexDataWrapperAccount = response.json().await?;
            // for p in payload.data.positions {
            //     println!("{}", p);           
            //     debug!("current mark to market is: ");
            //     let perps = get_perpetuals().await;
            //     let contract_size = perps.unwrap()[&p.symbol].contract_size;
            //     let pos = (p.size as f64 * contract_size) * p.mark_price;
            //     let upl = ((p.size as f64 * contract_size) * p.mark_price) - ((p.size as f64 * contract_size) * p.avg_entry_price);
            //     debug!("{:?} {:?}", pos, upl);
            // }


        },

        "hack" => {

            let foo = vec![2f64 * (10.-2.)];
            debug!("{:?}", foo);

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












