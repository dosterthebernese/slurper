// this use statement gets you access to the lib file
use slurper::*;

use std::time::Duration;

use log::{info,debug,warn};
use std::error::Error;
use self::models::{CoinMetrics,KafkaCryptoTrade,CryptoMarket};
use chrono::{DateTime,Utc,TimeZone,SecondsFormat};

use futures::stream::TryStreamExt;
use mongodb::{Client};
use mongodb::{bson::doc};
use mongodb::options::{FindOptions};

use kafka::error::Error as KafkaError;
use kafka::producer::{Producer, Record, RequiredAcks};


#[macro_use]
extern crate clap;
use clap::App;

extern crate serde;


use serde::Deserialize;
//use reqwest::Error;


use std::fmt; // Import `fmt`

const MARKETS_URL: &str = "https://community-api.coinmetrics.io/v4/catalog/markets";
const TS_URL: &str = "https://community-api.coinmetrics.io/v4/timeseries/market-trades";




#[derive(Deserialize, Debug)]
struct CMDataMarket {
    #[serde(rename = "market", default)]
    market: String,  
    #[serde(rename = "symbol", default)]
    symbol: String
}

#[derive(Deserialize, Debug)]
struct CMDataWrapperMarkets {
    #[serde(rename = "data", default)]
    cmds: Vec<CMDataMarket>
}



#[derive(Deserialize, Debug)]
struct CMDataTSMarketTrades {
    time: String,
    market: String,
    coin_metrics_id: String,
    amount: String,
    price: String,
    database_time: String,
    side: Option<String>
}




#[derive(Deserialize, Debug)]
struct CMDataWrapperTSMarketData {
    #[serde(rename = "data", default)]
    cmds: Vec<CMDataTSMarketTrades>,
    #[serde(rename = "next_page_token", default)]
    next_page_token: String,
    #[serde(rename = "next_page_url", default)]
    next_page_url: String,
}















async fn get_ts_market_trades<'a>(whole_url: &'a str) -> Result<CMDataWrapperTSMarketData, Box<dyn Error>> {

    let request_url_ts = format!("{}", whole_url);
    let response_ts = reqwest::get(&request_url_ts).await?;
    let payload_ts: CMDataWrapperTSMarketData = response_ts.json().await?;

    Ok(payload_ts)

}


async fn get_all_coinmetrics_markets() -> Result<Vec<CryptoMarket>, Box<dyn Error>> {

    let mut rvec = Vec::new();

    let request_url = format!("{}", MARKETS_URL);
    let response = reqwest::get(&request_url).await?;
    let payload: CMDataWrapperMarkets = response.json().await?;

    for cmditem in payload.cmds {
        let this_market = CryptoMarket {
            market: cmditem.market
        };
        rvec.push(this_market.clone());
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

    let client = Client::with_uri_str(LOCAL_MONGO).await?;
    let database = client.database(THE_DATABASE);
    let cmcollection = database.collection::<CoinMetrics>(THE_COINMETRICS_COLLECTION);

    match matches.value_of("INPUT").unwrap() {


        "ts-append-all-markets" => {


            let broker = "localhost:9092";
            let topic = "coinmetrics-markets";

            let mut producer = Producer::from_hosts(vec![broker.to_owned()])
                .with_ack_timeout(Duration::from_secs(1))
                .with_required_acks(RequiredAcks::One)
                .create()?;


            let all_markets = get_all_coinmetrics_markets().await?;

            for market in all_markets {
                println!("{}", market);
                let big_bang = market.get_last_updated_trade(&cmcollection).await?;
                let gtedate = big_bang.to_rfc3339_opts(SecondsFormat::Secs, true);
                let mut ts_url1 = format!("{}?start_time={}&paging_from=start&markets={}&page_size=1000", TS_URL,gtedate,market.market);
                let mut saved_big_bang = big_bang.clone();

                while ts_url1 != "" {
                    let payload_ts = get_ts_market_trades(&ts_url1).await?;
                    for cmditem in payload_ts.cmds {
                        let trade_date = DateTime::parse_from_rfc3339(&cmditem.time).unwrap();
                        if trade_date > big_bang { // this is redundant I think to the above gtedate

                            let kafka_trade_date = DateTime::parse_from_rfc3339(&cmditem.time).unwrap();
                            
                            let new_kct = KafkaCryptoTrade {
                                trade_date: &kafka_trade_date.with_timezone(&Utc).to_rfc3339_opts(SecondsFormat::Secs, true),
                                coin_metrics_id: &cmditem.coin_metrics_id,
                                price: cmditem.price.parse::<f64>().unwrap(),
                                quantity: cmditem.amount.parse::<f64>().unwrap(),
                                market: &cmditem.market,
                                tx_type: &cmditem.side.unwrap_or("SHIT NOT PROVIDED".to_string()),
                            };


                            saved_big_bang = trade_date.with_timezone(&Utc);
                            println!("{}", new_kct);
                            // let _result = collection.insert_one(new_crypto_trade, None).await?;                                    


//                                let encoded: Vec<u8> = bincode::serialize(&new_kct).unwrap();

                             let data = serde_json::to_string(&new_kct).expect("json serialization failed");
                             let data_as_bytes = data.as_bytes();
// //                                let data = new_crypto_trade.as_bytes();

                            producer.send(&Record {
                                topic,
                                partition: -1,
                                key: (),
                                value: data_as_bytes,
                            })?;


                        }
                    }
                    debug!("npt {:?}", payload_ts.next_page_token);
                    debug!("npu {:?}", payload_ts.next_page_url);
                    ts_url1 = payload_ts.next_page_url;
                    if &ts_url1 == "" {
                        debug!("last of the pagination {:?}", saved_big_bang);
                        let new_cm = CoinMetrics {
                            last_known_trade_date: saved_big_bang,
                            market: market.market.clone(),
                        };
                        let _result = cmcollection.insert_one(new_cm, None).await?;                                    
                    }
                }

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












