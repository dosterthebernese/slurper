// this use statement gets you access to the lib file
use slurper::*;

use log::{info,debug,warn};
use std::error::Error;
use std::convert::TryFrom;
use self::models::{CryptoTrade, CryptoLiquidation, CryptoMarket, CryptoAsset, CapSuite};
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


#[derive(Deserialize, Debug)]
struct PhemexProduct {
    #[serde(rename = "symbol", default)]
    symbol: String,
    #[serde(rename = "type", default)]
    product_type: String,
}
impl fmt::Display for PhemexProduct {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<20}", self.symbol, self.product_type)
    }
}


#[derive(Deserialize, Debug)]
struct PhemexCurrency {
    #[serde(rename = "currency", default)]
    currency: String,
    #[serde(rename = "name", default)]
    name: String,
}
impl fmt::Display for PhemexCurrency {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<20}", self.currency, self.name)
    }
}


#[derive(Deserialize, Debug)]   

struct PhemexData {
    #[serde(rename = "ratioScale", default)]
    ratio_scale: i32,
    #[serde(rename = "currencies", default)]
    currencies: Vec<PhemexCurrency>,
    #[serde(rename = "products", default)]
    products: Vec<PhemexProduct>
}


#[derive(Deserialize, Debug)]
struct PhemexDataWrapperProducts {
    code: i32,
    msg: String,
    data: PhemexData
}



#[derive(Deserialize, Debug)]
struct PhemexAccount {
    #[serde(rename = "accountId", default)]
    account_id: i32,
    #[serde(rename = "currency", default)]
    currency: String,
    #[serde(rename = "accountBalanceEv", default)]
    account_balance_ev: f64,
    #[serde(rename = "totalUsedBalanceEv", default)]
    total_used_balance_ev: f64,
}


#[derive(Deserialize, Debug)]
struct PhemexDataWrapperAccount {
    code: i32,
    msg: String,
    data: PhemexAccount
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

        "products" => {

            let request_url = format!("https://api.phemex.com/public/products");
            let response = reqwest::get(&request_url).await?;

            let payload: PhemexDataWrapperProducts = response.json().await?;

            for item in payload.data.currencies {
                println!("{}", item);
            }

            for item in payload.data.products {
                println!("{}", item);
            }

        },
        "assets" => {






            let exp = (Utc::now() + Duration::milliseconds(10000)).timestamp();
            let expstr = &exp.to_string();            
            let msg = format!("{}{}", "/accounts/accountPositionscurrency=USD",&expstr);


            let mut signed_key = Hmac::<Sha256>::new_from_slice("9B1Yl3NZV0DJS7Q3xJ9LRgfJEFwwdhST0Ihh0DBjePo5Y2I1MjNmZS1hOTkzLTQzNjMtODQ3MS03ZDY0N2M1ZTZmOTk".as_bytes()).unwrap();
            signed_key.update(msg.as_bytes());
            let signature = hex_encode(signed_key.finalize().into_bytes());            
            debug!("{:?}", signature);


            let client = reqwest::Client::builder().build()?;

//             let the_action = b"/accounts/accountPositions";
//             let the_parms = b"?currency=USD";
//             let expstr = (Utc::now() + Duration::milliseconds(10000)).timestamp().to_string();
//             let exps = exp.to_be_bytes();
//             debug!("{:?}", rawstring);

//             let apisecret = b"9B1Yl3NZV0DJS7Q3xJ9LRgfJEFwwdhST0Ihh0DBjePo5Y2I1MjNmZS1hOTkzLTQzNjMtODQ3MS03ZDY0N2M1ZTZmOTk"; 
//             let bytes = base64::decode("9B1Yl3NZV0DJS7Q3xJ9LRgfJEFwwdhST0Ihh0DBjePo5Y2I1MjNmZS1hOTkzLTQzNjMtODQ3MS03ZDY0N2M1ZTZmOTk").unwrap();

//             debug!("{:?}\n{:?}\n", apisecret, bytes);

//             // let mut mac = HmacSha256::new_from_slice(apisecret);
//             let mut mac = HmacSha256::new_from_slice(&bytes).expect("yo mamma");
//             mac.update(rawstring.as_bytes());

//             // mac.update(the_parms);
//             // mac.update(&exps);
// //            let result = mac.finalize();

// //            let result_hash: String = format!("{:X}", mac.finalize().into_bytes());

// //            debug!("{:?}", ip_address_hash);    

//             let result = mac.finalize().into_bytes();
//             debug!("{:?}", result);

//             let val = HeaderValue::from_bytes(&result).unwrap();

            // debug!("{:?}", result.code());


            // headers.insert("", result.to_string().parse().unwrap());



            let request_url = format!("https://vapi.phemex.com/accounts/accountPositions?currency=USD");
            let response = client.get(&request_url).header("x-phemex-access-token", "89124f02-5e64-436a-bb3c-a5f4d720664d")
            .header("x-phemex-request-expiry", exp).header("x-phemex-request-signature", signature).send().await?;


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












