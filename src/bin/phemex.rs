// this use statement gets you access to the lib file
use slurper::*;

use log::{info,debug,warn};
use std::error::Error;
use std::convert::TryFrom;
use self::models::{PhemexDataWrapperAccount,PhemexDataWrapperProducts};
use chrono::{DateTime,Utc,TimeZone,SecondsFormat};
use time::Duration;

use futures::stream::TryStreamExt;
use mongodb::{Client};
use mongodb::{bson::doc};
use mongodb::options::{FindOptions};

use http::header::HeaderValue;

#[macro_use]
extern crate clap;
use clap::App;

extern crate serde;
extern crate base64;



use hex::encode as hex_encode;
use hmac::{Hmac, Mac, NewMac};
use sha2::Sha256;

use serde::Deserialize;
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

        "products" => {

            let request_url = format!("{}", PRODUCTS_URL);
            let response = reqwest::get(&request_url).await?;

            let payload: PhemexDataWrapperProducts = response.json().await?;

            for item in payload.data.currencies {
                println!("{}", item);
            }

            for item in payload.data.products {
                println!("{}", item);
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
            debug!("{:?}", payload);


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












