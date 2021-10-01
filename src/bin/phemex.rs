// this use statement gets you access to the lib file
use slurper::*;

use log::{info,debug,warn};
use std::error::Error;
use std::convert::TryFrom;
use self::models::{PhemexDataWrapperAccount,PhemexDataWrapperProducts, PhemexProduct, PhemexCurrency};
use chrono::{DateTime,Utc,TimeZone,SecondsFormat};
use time::Duration;

use futures::stream::TryStreamExt;
use mongodb::{Client};
use mongodb::{bson::doc};
use mongodb::options::{FindOptions};

use http::header::HeaderValue;

use std::iter::FromIterator;
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
//use serde_json::Result;
use serde_json::json;
use reqwest::header::HeaderMap;
//use reqwest::Error;


use std::fmt; // Import `fmt`

// Create alias for HMAC-SHA256
//type HmacSha256 = Hmac<Sha256>;


// i know bad but will change, only works on my IP, and there's 20 bucks in the account
const API_TOKEN: &str = "89124f02-5e64-436a-bb3c-a5f4d720664d";
const API_SECRET: &str = "9B1Yl3NZV0DJS7Q3xJ9LRgfJEFwwdhST0Ihh0DBjePo5Y2I1MjNmZS1hOTkzLTQzNjMtODQ3MS03ZDY0N2M1ZTZmOTk";


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


#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {

    env_logger::init(); 


    let yaml = load_yaml!("../coinmetrics.yml");
    let matches = App::from_yaml(yaml).get_matches();
//    debug!("{:?}",matches);


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
            for (symbol, product) in get_perpetuals().await.unwrap() {
                println!("{}", product);
            }
        },


        "account-positions" => {

            info!("this queries all positions in account");
            let exp = (Utc::now() + Duration::milliseconds(10000)).timestamp();
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
                debug!("current mark to market is: ");
                let perps = get_perpetuals().await;
                let contract_size = perps.unwrap()[&p.symbol].contract_size;
                let pos = (p.size as f64 * contract_size) * p.mark_price;
                let upl = ((p.size as f64 * contract_size) * p.mark_price) - ((p.size as f64 * contract_size) * p.avg_entry_price);
                debug!("{:?} {:?}", pos, upl);
            }


        },


        "place-order" => {

            info!("currently hardcoded");
            let exp = (Utc::now() + Duration::milliseconds(10000)).timestamp();
            let expstr = &exp.to_string();            

            let order = Order {
                action_by: "FromOrderPlacement".to_string(),
                symbol: "ETHUSD".to_string(),
                client_order_id: "".to_string(),
                side: "Sell".to_string(),
                price_ep: 32270000,
                quantity: 16.0,
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


        }







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












