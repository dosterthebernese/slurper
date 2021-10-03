// this use statement gets you access to the lib file
use slurper::*;

use log::{info,debug,warn};
use std::error::Error;
use self::models::{CryptoTrade, CryptoLiquidation, CryptoMarket, CryptoAsset, CapSuite};
use chrono::{DateTime,Utc,TimeZone,SecondsFormat};

use futures::stream::TryStreamExt;
use mongodb::{Client};
use mongodb::{bson::doc};
use mongodb::options::{FindOptions};

#[macro_use]
extern crate clap;
use clap::App;

extern crate serde;


use serde::Deserialize;
//use reqwest::Error;


use std::fmt; // Import `fmt`

const MARKETS_URL: &str = "https://community-api.coinmetrics.io/v4/catalog/markets";
const TS_URL: &str = "https://community-api.coinmetrics.io/v4/timeseries/market-trades";
const TS_LIQ_URL: &str = "https://community-api.coinmetrics.io/v4/timeseries/market-liquidations";

#[derive(Deserialize, Debug)]
struct CMDataMetric {
    metric: String,
    full_name: String,
    description: String
}
impl fmt::Display for CMDataMetric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<20} {:<20}\n{:<100}\n\n", 
            self.metric, self.full_name, self.description)
    }
}


#[derive(Deserialize, Debug)]
struct CMDataWrapperMetrics {
    #[serde(rename = "data", default)]
    cmds: Vec<CMDataMetric>
}




#[derive(Deserialize, Debug)]
struct Metric {
    #[serde(rename = "metric", default)]
    metric: String,
}



#[derive(Deserialize, Debug)]
struct CMDataAsset {
    asset: String,
    full_name: String,
    #[serde(rename = "metrics", default)]
    metrics: Vec<Metric>,
    #[serde(rename = "exchanges", default)]
    exchanges: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct CMDataWrapperAssets {
    #[serde(rename = "data", default)]
    cmds: Vec<CMDataAsset>
}


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

impl CMDataTSMarketTrades {


    fn make_crypto_trade(self: &Self) -> CryptoTrade {

        let this_eth_market = CryptoMarket {
            market: self.market.clone()
        };

        let trade_date = DateTime::parse_from_rfc3339(&self.time).unwrap();
        
        CryptoTrade {
            trade_date: trade_date.with_timezone(&Utc),
            coin_metrics_id: self.coin_metrics_id.clone(),
            price: self.price.parse::<f64>().unwrap(),
            quantity: self.amount.parse::<f64>().unwrap(),
            market: self.market.clone(),
            tx_type: self.side.clone().unwrap_or("NOT PROVIDED".to_string()),
            trade_llama_exchange: this_eth_market.just_exchange().unwrap().to_string(),
            trade_llama_instrument: this_eth_market.drop_exchange().unwrap(),
            trade_llama_instrument_type: this_eth_market.drop_all_but_instrument_type().unwrap().to_string()                                                        
        }

    }

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










#[derive(Deserialize, Debug)]
struct CMDataTSMarketLiquidations {
    time: String,
    market: String,
    coin_metrics_id: String,
    amount: String,
    price: String,
    #[serde(rename = "type", default)]
    cm_type: Option<String>,
    database_time: String,
    side: Option<String>
}


impl CMDataTSMarketLiquidations {

    fn make_crypto_liquidation(self: &Self) -> CryptoLiquidation {

        let this_eth_market = CryptoMarket {
            market: self.market.clone()
        };

        let trade_date = DateTime::parse_from_rfc3339(&self.time).unwrap();
        
        CryptoLiquidation {
            trade_date: trade_date.with_timezone(&Utc),
            coin_metrics_id: self.coin_metrics_id.clone(),
            price: self.price.parse::<f64>().unwrap(),
            quantity: self.amount.parse::<f64>().unwrap(),
            market: self.market.clone(),
            tx_type: self.side.clone().unwrap_or("NOT PROVIDED".to_string()),
            cm_type: self.cm_type.clone().unwrap_or("NOT PROVIDED".to_string()),
            trade_llama_exchange: this_eth_market.just_exchange().unwrap().to_string(),
            trade_llama_instrument: this_eth_market.drop_exchange().unwrap(),
            trade_llama_instrument_type: this_eth_market.drop_all_but_instrument_type().unwrap().to_string()                                                        
        }

    }

}







#[derive(Deserialize, Debug)]
struct CMDataWrapperTSMarketLiquidations {
    #[serde(rename = "data", default)]
    cmds: Vec<CMDataTSMarketLiquidations>,
    #[serde(rename = "next_page_token", default)]
    next_page_token: String,
    #[serde(rename = "next_page_url", default)]
    next_page_url: String,
}


#[derive(Deserialize, Debug)]
struct CMDataTSAssetMetricsCapSuite {
    asset: String,
    time: String,
    #[serde(rename = "PriceUSD", default)]
    price: Option<String>,
    #[serde(rename = "CapMVRVCur", default)]
    cap_mvrv_cur: Option<String>,
    #[serde(rename = "CapMVRVFF", default)]
    cap_mvrv_ff: Option<String>,
    #[serde(rename = "CapMrktCurUSD", default)]
    cap_mrkt_cur_usd: Option<String>,
    #[serde(rename = "CapMrktFFUSD", default)]
    cap_mrkt_ff_usd: Option<String>,
    #[serde(rename = "CapRealUSD", default)]
    cap_real_usd: Option<String>
}

#[derive(Deserialize, Debug)]
struct CMDataWrapperTSAssetMetricsCapSuite {
    #[serde(rename = "data", default)]
    cmds: Vec<CMDataTSAssetMetricsCapSuite>,
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

async fn get_ts_market_liquidations<'a>(whole_url: &'a str) -> Result<CMDataWrapperTSMarketLiquidations, Box<dyn Error>> {

    let request_url_ts = format!("{}", whole_url);
    let response_ts = reqwest::get(&request_url_ts).await?;
    let payload_ts: CMDataWrapperTSMarketLiquidations = response_ts.json().await?;

    Ok(payload_ts)

}

async fn get_ts_asset_metrics_cap_suite_eth<'a>(whole_url: &'a str) -> Result<CMDataWrapperTSAssetMetricsCapSuite, Box<dyn Error>> {

    let request_url_ts = format!("{}", whole_url);
    let response_ts = reqwest::get(&request_url_ts).await?;
    let payload_ts: CMDataWrapperTSAssetMetricsCapSuite = response_ts.json().await?;

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
    let collection = database.collection::<CryptoTrade>(THE_CRYPTO_COLLECTION);
    let lcollection = database.collection::<CryptoLiquidation>(THE_CRYPTO_LIQUIDATION_COLLECTION);
    let capcollection = database.collection::<CapSuite>(THE_CRYPTO_CAP_SUITE_COLLECTION);

    match matches.value_of("INPUT").unwrap() {

        "assets" => {

            let request_url = format!("https://community-api.coinmetrics.io/v4/catalog/assets");
            let response = reqwest::get(&request_url).await?;

            let payload: CMDataWrapperAssets = response.json().await?;

            for cmditem in payload.cmds {
                debug!("{:?} {:?}", cmditem.asset, cmditem.full_name);
                debug!("Metrics Below:");
                for metric in cmditem.metrics {
                    debug!("   {:?}", metric.metric);
                }
                debug!("Exchanges Below:");
                for exchange in cmditem.exchanges {
                    debug!("   {:?}", exchange);
                }
            }

        },        

        "metrics" => {
            let request_url = format!("https://community-api.coinmetrics.io/v4/catalog/metrics");
            let response = reqwest::get(&request_url).await?;
            let payload: CMDataWrapperMetrics = response.json().await?;
            for cmditem in payload.cmds {
                println!("{}",cmditem);
            }
        },        




        "markets" => {

            let all_markets = get_all_coinmetrics_markets().await?;
            for market in all_markets {
                println!("{}", market);
            }

        },


        "ts-append-ETH-cap-suite" => {

            let this_eth_asset = CryptoAsset {
                asset: "ETH".to_string()
            };

            let big_bang = this_eth_asset.get_last_updated_cap_suite(&capcollection).await?;
            info!("I am going to ignore anything prior to {:?} for {:?}", &big_bang, &this_eth_asset.asset);
            let gtedate = big_bang.to_rfc3339_opts(SecondsFormat::Secs, true);
            debug!("Our start time for the query is: {:?} for asset {:?}", gtedate, &this_eth_asset.asset);


            let mut ts_url1 = format!("https://community-api.coinmetrics.io/v4/timeseries/asset-metrics?start_time={}&paging_from=start&assets={}&metrics={}&page_size=1000", gtedate, this_eth_asset.asset,"PriceUSD,CapMVRVCur,CapMVRVFF,CapMrktCurUSD,CapMrktFFUSD,CapRealUSD");
            let payload_ts = get_ts_asset_metrics_cap_suite_eth(&ts_url1).await?;

            for cmditem in payload_ts.cmds {

                let trade_date = DateTime::parse_from_rfc3339(&cmditem.time).unwrap();
                if trade_date > big_bang {

                    match (cmditem.price, cmditem.cap_mvrv_cur, cmditem.cap_mvrv_ff, cmditem.cap_mrkt_cur_usd, cmditem.cap_mrkt_ff_usd, cmditem.cap_real_usd) {
                        (Some(price), Some(cap_mvrv_cur), Some(cap_mvrv_ff), Some(cap_mrkt_cur_usd), Some(cap_mrkt_ff_usd), Some(cap_real_usd)) => {

                            let new_cap_suite = CapSuite {
                                asset: this_eth_asset.asset.clone(),
                                trade_date: trade_date.with_timezone(&Utc),
                                price: price.parse::<f64>().unwrap(),
                                cap_mvrv_cur: cap_mvrv_cur.parse::<f64>().unwrap(),
                                cap_mvrv_ff: cap_mvrv_ff.parse::<f64>().unwrap(),
                                cap_mrkt_cur_usd: cap_mrkt_cur_usd.parse::<f64>().unwrap(),
                                cap_mrkt_ff_usd: cap_mrkt_ff_usd.parse::<f64>().unwrap(),
                                cap_real_usd: cap_real_usd.parse::<f64>().unwrap()
                            };
                            println!("{}", new_cap_suite);
                            let _result = capcollection.insert_one(new_cap_suite, None).await?;                                    
                        },
                        _ => {
                            warn!("problem, some of the cap vals do not exist, skipping - could be just a new set")
                        }

                    };

                }

            }

            debug!("npt {:?}", payload_ts.next_page_token);
            debug!("npu {:?}", payload_ts.next_page_url);
            ts_url1 = payload_ts.next_page_url;

            while ts_url1 != "" {
                let inner_payload_ts = get_ts_asset_metrics_cap_suite_eth(&ts_url1).await?;
                for cmditem in inner_payload_ts.cmds {
                    let trade_date = DateTime::parse_from_rfc3339(&cmditem.time).unwrap();
                    if trade_date > big_bang {
                        match (cmditem.price, cmditem.cap_mvrv_cur, cmditem.cap_mvrv_ff, cmditem.cap_mrkt_cur_usd, cmditem.cap_mrkt_ff_usd, cmditem.cap_real_usd) {
                            (Some(price), Some(cap_mvrv_cur), Some(cap_mvrv_ff), Some(cap_mrkt_cur_usd), Some(cap_mrkt_ff_usd), Some(cap_real_usd)) => {

                                let new_cap_suite = CapSuite {
                                    asset: this_eth_asset.asset.clone(),
                                    trade_date: trade_date.with_timezone(&Utc),
                                    price: price.parse::<f64>().unwrap(),                                
                                    cap_mvrv_cur: cap_mvrv_cur.parse::<f64>().unwrap(),
                                    cap_mvrv_ff: cap_mvrv_ff.parse::<f64>().unwrap(),
                                    cap_mrkt_cur_usd: cap_mrkt_cur_usd.parse::<f64>().unwrap(),
                                    cap_mrkt_ff_usd: cap_mrkt_ff_usd.parse::<f64>().unwrap(),
                                    cap_real_usd: cap_real_usd.parse::<f64>().unwrap()
                                };
                                println!("{}", new_cap_suite);
                                let _result = capcollection.insert_one(new_cap_suite, None).await?;                                    
                            },
                            _ => {
                                warn!("problem, some of the cap vals do not exist, skipping - could be just a new set")
                            }

                        };

                    }
                }
                debug!("npt {:?}", inner_payload_ts.next_page_token);
                debug!("npu {:?}", inner_payload_ts.next_page_url);
                ts_url1 = inner_payload_ts.next_page_url;
            }

        },


        "ts-append-ETH-USD-liquidations" => {

            let all_markets = get_all_coinmetrics_markets().await?;
            for market in all_markets {
                if market.is_eth_futures_market() {
                    println!("{}", market);
                    let big_bang = market.get_last_updated_liquidation(&lcollection).await?;
                    let gtedate = big_bang.to_rfc3339_opts(SecondsFormat::Secs, true);
                    let mut ts_url1 = format!("{}?start_time={}&paging_from=start&markets={}&page_size=1000", TS_LIQ_URL,gtedate,market.market);

                    while ts_url1 != "" {
                        let inner_payload_ts = get_ts_market_liquidations(&ts_url1).await?;
                        for cmditem in inner_payload_ts.cmds {
                            let trade_date = DateTime::parse_from_rfc3339(&cmditem.time).unwrap();
                            if trade_date > big_bang {
                                let new_crypto_liq = cmditem.make_crypto_liquidation();
                                println!("{}", new_crypto_liq);
                                let _result = lcollection.insert_one(new_crypto_liq, None).await?;                                    
                            }
                        }
                        debug!("npt {:?}", inner_payload_ts.next_page_token);
                        debug!("npu {:?}", inner_payload_ts.next_page_url);
                        ts_url1 = inner_payload_ts.next_page_url;
                    }

                }
            }
        },



        "ts-append-eth-usd" => {

            let all_markets = get_all_coinmetrics_markets().await?;

            for market in all_markets {

                if market.is_eth_market() {
                    println!("{}", market);
                    let big_bang = market.get_last_updated_trade(&collection).await?;
                    let gtedate = big_bang.to_rfc3339_opts(SecondsFormat::Secs, true);
                    let mut ts_url1 = format!("{}?start_time={}&paging_from=start&markets={}&page_size=1000", TS_URL,gtedate,market.market);
                    while ts_url1 != "" {
                        let payload_ts = get_ts_market_trades(&ts_url1).await?;
                        for cmditem in payload_ts.cmds {
                            let trade_date = DateTime::parse_from_rfc3339(&cmditem.time).unwrap();
                            if trade_date > big_bang { // this is redundant I think to the above gtedate
                                let new_crypto_trade = cmditem.make_crypto_trade();
                                println!("{}", new_crypto_trade);
                                let _result = collection.insert_one(new_crypto_trade, None).await?;                                    
                            }
                        }
                        debug!("npt {:?}", payload_ts.next_page_token);
                        debug!("npu {:?}", payload_ts.next_page_url);
                        ts_url1 = payload_ts.next_page_url;
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












