use crate::*;

use bson::serde_helpers::chrono_datetime_as_bson_datetime;
use std::time::Duration as HackDuration;
use std::collections::HashMap;
use serde::{Serialize,Deserialize};
//use mongodb::{bson::doc};
use std::fmt; // Import `fmt`
use futures::stream::TryStreamExt;
use mongodb::error::Error as MongoError;
use mongodb::{Client,Collection};
use mongodb::{bson::doc};
use mongodb::options::{FindOptions,FindOneOptions};
use time::Duration;
use std::convert::TryFrom;




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

pub async fn get_perpetuals() -> Result<HashMap<String, PhemexProduct>, Box<dyn Error>> {

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


pub async fn get_products() -> Result<Vec<PhemexProduct>, Box<dyn Error>> {

     let mut rvec = Vec::new();

    let request_url = format!("{}", PRODUCTS_URL);
    let response = reqwest::get(&request_url).await?;

    let payload: PhemexDataWrapperProducts = response.json().await?;
    for item in payload.data.products {
        rvec.push(item);
    }

    Ok(rvec)
} 

pub async fn get_currencies() -> Result<Vec<PhemexCurrency>, Box<dyn Error>> {

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



pub async fn poc2() -> Result<(), Box<dyn Error>> {


    let broker = "localhost:9092";
    let topic = "phemex-perpetuals-open-interest";

    let mut producer = Producer::from_hosts(vec![broker.to_owned()])
        .with_ack_timeout(HackDuration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()?;



    let the_eth_boys = vec!["ETHUSD","BTCUSD","AAVEUSD","SOLUSD","UNIUSD"];
    info!("this process should be daemonized");
    let mut interval = TokioTime::interval(TokioDuration::from_millis(1000));

    let mut tmpcnt = 0;
    loop {
        if tmpcnt == 1000000 {
            break;
        } else {
            tmpcnt+=1;
        }
        for eth in &the_eth_boys {
            let current_md = get_market_data(eth).await?;

            let new_tlphsnap = TLPhemexMDSnapshot {
                snapshot_date: &Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                symbol: eth,
                open: current_md.open,
                high: current_md.high,
                low: current_md.low,
                close: current_md.close,
                index_price: current_md.index_price,
                mark_price: current_md.mark_price,
                open_interest: current_md.open_interest
            };
            println!("{}", new_tlphsnap);
//                    let _result = tlphsnapcollection.insert_one(&new_tlphsnap, None).await?;                                                                                


            let data = serde_json::to_string(&new_tlphsnap).expect("json serialization failed");
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


    Ok(())


}



pub async fn poc<'a>(api_secret: &'a str, api_token: &'a str) -> Result<(), Box<dyn Error>> {

    info!("this queries all positions in account");
    let exp = (Utc::now() + NormalDuration::milliseconds(10000)).timestamp();
    let expstr = &exp.to_string();            

    let request_url = format!("{}",phemex::ACCOUNT_POSTIONS_URL);
    let msg = format!("{}{}", ACCOUNT_POSTIONS_MSG,&expstr);

    let mut signed_key = Hmac::<Sha256>::new_from_slice(api_secret.as_bytes()).unwrap();
    signed_key.update(msg.as_bytes());
    let signature = hex_encode(signed_key.finalize().into_bytes());            

    let client = reqwest::Client::builder().build()?;
    let response = client.get(&request_url).header("x-phemex-access-token", api_token).header("x-phemex-request-expiry", exp).header("x-phemex-request-signature", signature).send().await?;

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

        // let new_tlphsnap = TLPhemexMDSnapshot {
        //     snapshot_date: Utc::now(),
        //     symbol: p.symbol,
        //     open: current_md.open,
        //     high: current_md.high,
        //     low: current_md.low,
        //     close: current_md.close,
        //     index_price: current_md.index_price,
        //     mark_price: current_md.mark_price,
        //     open_interest: current_md.open_interest
        // };

        // println!("{}", new_tlphsnap);
        // let _result = tlphsnapcollection.insert_one(&new_tlphsnap, None).await?;                                                                
        // let tlphsnhist = new_tlphsnap.get_history(&LOOKBACK_OPEN_INTEREST,&tlphsnapcollection).await?;

        // let disasf_prices = delta_walk_integers(&tlphsnhist.iter().map(|n| n.mark_price).collect::<Vec<i64>>());
        // let disasf_open_interest = delta_walk_integers(&tlphsnhist.iter().map(|n| n.open_interest).collect::<Vec<i64>>());
        // let its_snapshots = interval_walk_timestamps(&tlphsnhist.iter().map(|n| n.snapshot_date).collect::<Vec<DateTime<Utc>>>());

        // for (idx, q) in tlphsnhist.iter().enumerate() {
        //     println!("{} {:>9.4} {:>9.4} {}", q, disasf_prices[idx], disasf_open_interest[idx], its_snapshots[idx]);
        // }

        // debug!("open interest delta {:?}", &new_tlphsnap.get_open_interest_delta(&LOOKBACK_OPEN_INTEREST, &tlphsnapcollection).await?);
        // debug!("mark price delta {:?}", &new_tlphsnap.get_mark_price_delta(&LOOKBACK_OPEN_INTEREST, &tlphsnapcollection).await?);

        // let lh = &new_tlphsnap.get_liquidations(&LOOKBACK_OPEN_INTEREST, &lcollection).await?;
        // let new_trades = Trades {
        //     vts: lh.clone()
        // };

        // debug!("total volume of liquidations for the last 26 hours is: {}", new_trades.get_total_volume().unwrap());
        // let (p,q,pq,rkm) = new_trades.get_pqkm().unwrap();

        // for (idx, km) in rkm.iter().enumerate() {
        //     debug!("{:?} {:?} {:?}", p[idx],q[idx],km);
        // }


    }


    Ok(())


}




















#[derive(Deserialize, Debug)]
pub struct PhemexProduct {
    #[serde(rename = "symbol", default)]
    pub symbol: String,
    #[serde(rename = "type", default)]
    pub product_type: String,
    #[serde(rename = "displaySymbol", default)]
    pub display_symbol: String,
    #[serde(rename = "indexSymbol", default)]
    pub index_symbol: String,
    #[serde(rename = "markSymbol", default)]
    pub mark_symbol: String,
    #[serde(rename = "fundingRateSymbol", default)]
    pub funding_rate_symbol: String,
    #[serde(rename = "fundingRate8hSymbol", default)]
    pub funding_rate_eight_hour_symbol: String,
    #[serde(rename = "contractUnderlyingAssets", default)]
    pub contract_underlying_assets: String,
    #[serde(rename = "settleCurrency", default)]
    pub settle_currency: String,
    #[serde(rename = "quoteCurrency", default)]
    pub quote_currency: String,
    #[serde(rename = "contractSize", default)]
    pub contract_size: f64,
}
impl fmt::Display for PhemexProduct {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<30} {:<10} {:<10} {:<10} {:<10} {:>10}", self.symbol, self.product_type, 
            self.display_symbol, self.index_symbol, self.mark_symbol, self.contract_underlying_assets,
            self.contract_size)
    }
}


#[derive(Deserialize, Debug)]
pub struct PhemexCurrency {
    #[serde(rename = "currency", default)]
    pub currency: String,
    #[serde(rename = "name", default)]
    pub name: String,
    #[serde(rename = "valueScale", default)]
    pub value_scale: i64,
    #[serde(rename = "minValueEv", default)]
    pub min_value_ev: i64,
    #[serde(rename = "maxValueEv", default)]
    pub max_value_ev: i64,
}
impl fmt::Display for PhemexCurrency {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<20} {:>10} {:>10} {:>10}", self.currency, self.name, self.value_scale, self.min_value_ev, self.max_value_ev)
    }
}


#[derive(Deserialize, Debug)]   
pub struct PhemexData {
    #[serde(rename = "ratioScale", default)]
    ratio_scale: i32,
    #[serde(rename = "currencies", default)]
    pub currencies: Vec<PhemexCurrency>,
    #[serde(rename = "products", default)]
    pub products: Vec<PhemexProduct>
}


#[derive(Deserialize, Debug)]
pub struct PhemexDataWrapperProducts {
    code: i32,
    msg: String,
    pub data: PhemexData
}






#[derive(Deserialize, Debug)]
pub struct PhemexPosition {
    #[serde(rename = "accountId", default)]
    pub account_id: i32,
    #[serde(rename = "symbol", default)]
    pub symbol: String,
    #[serde(rename = "currency", default)]
    pub currency: String,
    #[serde(rename = "side", default)]
    pub side: String,
    #[serde(rename = "positionStatus", default)]
    pub position_status: String,
    #[serde(rename = "crossMargin", default)]
    pub cross_margin: bool,
    #[serde(rename = "leverageEr", default)]
    pub leverage_er: i64,
    #[serde(rename = "initMarginReqEr", default)]
    pub init_margin_req_er: i64,
    #[serde(rename = "initMarginReq", default)]
    pub init_margin_req: f64,
    #[serde(rename = "maintMarginReqEr", default)]
    pub maint_margin_req_er: i64,
    #[serde(rename = "maintMarginReq", default)]
    pub maint_margin_req: f64,
    #[serde(rename = "riskLimitEv", default)]
    pub risk_limit_ev: i64,
    #[serde(rename = "size", default)]
    pub size: i64,
    #[serde(rename = "valueEv", default)]
    pub value_ev: i64,
    #[serde(rename = "avgEntryPriceEp", default)]
    pub avg_entry_price_ep: i64,
    #[serde(rename = "avgEntryPrice", default)]
    pub avg_entry_price: f64,
    #[serde(rename = "posCostEv", default)]
    pub pos_cost_ev: i64,
    #[serde(rename = "assignedPosBalanceEv", default)]
    pub assigned_pos_balance_ev: i64,
    #[serde(rename = "bankruptCommEv", default)]
    pub bankrupt_comm_ev: i64,
    #[serde(rename = "bankruptPriceEp", default)]
    pub bankrupt_price_ep: i64,
    #[serde(rename = "positionMarginEv", default)]
    pub position_margin_ev: i64,
    #[serde(rename = "liquidationPriceEp", default)]
    pub liquidation_price_ep: i64,
    #[serde(rename = "deleveragePercentileEr", default)]
    pub deleverage_percentile_er: i64,
    #[serde(rename = "buyValueToCostEr", default)]
    pub buy_value_to_cost_er: i64,
    #[serde(rename = "sellValueToCostEr", default)]
    pub sell_value_to_cost_er: i64,
    #[serde(rename = "markPriceEp", default)]
    pub mark_price_ep: i64,
    #[serde(rename = "markPrice", default)]
    pub mark_price: f64,
    #[serde(rename = "markValueEv", default)]
    pub mark_value_ev: i64,
    #[serde(rename = "unRealisedPosLossEv", default)]
    pub unrealised_pos_loss_ev: i64,
    #[serde(rename = "estimatedOrdLossEv", default)]
    pub estimated_ord_loss_ev: i64,
    #[serde(rename = "usedBalanceEv", default)]
    pub used_balance_ev: i64,
    #[serde(rename = "takeProfitEp", default)]
    pub take_profit_ep: i64,
    #[serde(rename = "stopLossEp", default)]
    pub stop_loss_ep: i64,
    #[serde(rename = "cumClosedPnlEv", default)]
    pub cum_closed_pnl_ev: i64,
    #[serde(rename = "cumFundingFeeEv", default)]
    pub cum_funding_fee_ev: i64,
    #[serde(rename = "cumTransactFeeEv", default)]
    pub cum_transact_fee_ev: i64,
    #[serde(rename = "realisedPnlEv", default)]
    pub realised_pnl_ev: i64,
    #[serde(rename = "unRealisedPnlEv", default)]
    pub unrealised_pnl_ev: i64,
    #[serde(rename = "cumRealisedPnlEv", default)]
    pub cum_realised_pnl_ev: i64,
}

impl fmt::Display for PhemexPosition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<10} {:>10} {:>10} {:>10.4}", self.symbol, self.side, self.size, self.mark_price_ep, self.mark_price)
    }
}



#[derive(Deserialize, Debug)]
pub struct PhemexAccount {
    #[serde(rename = "accountId", default)]
    pub account_id: i32,
    #[serde(rename = "currency", default)]
    pub currency: String,
    #[serde(rename = "accountBalanceEv", default)]
    pub account_balance_ev: f64,
    #[serde(rename = "totalUsedBalanceEv", default)]
    pub total_used_balance_ev: f64,
    #[serde(rename = "positions", default)]
    pub positions: Vec<PhemexPosition>,
}


#[derive(Deserialize, Debug)]
pub struct PhemexDataWrapperAccount {
    code: i32,
    msg: String,
    pub data: PhemexAccount
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TLPhemexMDSnapshot<'a> {
    pub snapshot_date: &'a str,
    pub symbol: &'a str,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub index_price: i64,
    pub mark_price: i64,
    pub open_interest: i64,
}


impl fmt::Display for TLPhemexMDSnapshot<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<10} {:<10} {:<10}", self.snapshot_date, self.symbol, self.mark_price, self.open_interest)
    }
}






#[derive(Deserialize, Debug)]
pub struct PhemexMD {
    #[serde(rename = "open", default)]
    pub open: i64,
    #[serde(rename = "high", default)]
    pub high: i64,
    #[serde(rename = "low", default)]
    pub low: i64,
    #[serde(rename = "close", default)]
    pub close: i64,
    #[serde(rename = "indexPrice", default)]
    pub index_price: i64,
    #[serde(rename = "markPrice", default)]
    pub mark_price: i64,
    #[serde(rename = "openInterest", default)]
    pub open_interest: i64,
}


#[derive(Deserialize, Debug)]
pub struct PhemexDataWrapperMD {
    error: Option<String>,
    id: i32,
    pub result: PhemexMD
}






//         "place-order" => {

//             info!("currently hardcoded");
//             let exp = (Utc::now() + NormalDuration::milliseconds(10000)).timestamp();
//             let expstr = &exp.to_string();            

//             let order = Order {
//                 action_by: "FromOrderPlacement".to_string(),
//                 symbol: "ETHUSD".to_string(),
//                 client_order_id: "".to_string(),
//                 side: "Sell".to_string(),
//                 price_ep: 32600000,
//                 quantity: 5.0,
//                 order_type: "Limit".to_string()
//             };

//             let params = serde_json::to_string(&order)?;

//             debug!("{}", params);

//             let request_url = format!("{}",PLACE_ORDER_URL);
// //            let request_url = format!("{}","https://httpbin.org/post");


//             let msg = format!("{}{}{}", PLACE_ORDER_MSG,&expstr,params);
//             debug!("{}",msg);

//             let mut signed_key = Hmac::<Sha256>::new_from_slice(api_secret.as_bytes()).unwrap();
//             signed_key.update(msg.as_bytes());
//             let signature = hex_encode(signed_key.finalize().into_bytes());            

//             let client = reqwest::Client::builder().build()?;
//             let response = client.post(&request_url).header("x-phemex-access-token", api_token).header("x-phemex-request-expiry", exp).header("x-phemex-request-signature", signature).json(&params).body(params.to_owned()).send().await?;

//             let payload = response.text().await?;
//             debug!("{:?}", payload);


//             // let payload: PhemexDataWrapperAccount = response.json().await?;
//             // for p in payload.data.positions {
//             //     println!("{}", p);           
//             //     debug!("current mark to market is: ");
//             //     let perps = get_perpetuals().await;
//             //     let contract_size = perps.unwrap()[&p.symbol].contract_size;
//             //     let pos = (p.size as f64 * contract_size) * p.mark_price;
//             //     let upl = ((p.size as f64 * contract_size) * p.mark_price) - ((p.size as f64 * contract_size) * p.avg_entry_price);
//             //     debug!("{:?} {:?}", pos, upl);
//             // }


//         },




