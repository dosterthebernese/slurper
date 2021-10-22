// this use statement gets you access to the lib file - you use crate instead of the package name, who the fuck knows why (see any bin rs)
use crate::*;

use std::collections::HashMap;

use serde::{Serialize,Deserialize};
use bson::serde_helpers::chrono_datetime_as_bson_datetime;
use chrono::{DateTime,Utc};
use chrono::format::ParseError;

use mongodb::{Collection};
use mongodb::{error::Error};
use mongodb::{bson::doc};
use mongodb::options::{FindOptions};
use futures::stream::TryStreamExt;

use time::Duration;
use average::{WeightedMean,Min,Max};


use std::fmt; // Import `fmt`
use std::fmt::Error as NormalError;


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CryptoMarket<'a> {
    pub market: &'a str,
}

impl CryptoMarket<'_> {

    pub fn is_hotlist(self: &Self) -> bool {
        match self.get_coin().unwrap().to_lowercase().as_ref() {
            "aave" => true,
            "bal" => true,
            "matic" => true,
            "mkr" => true,
            "sol" => true,
            "uni" => true,
            _ => false
        } 
    }

    pub async fn get_last_updated_trade(self: &Self, collection: &Collection<CoinMetrics>) -> Result<DateTime<Utc>, Error> {

        let filter = doc! {"market": &self.market};
        let find_options = FindOptions::builder().sort(doc! { "trade_date":-1}).limit(1).build();
        let mut cursor = collection.find(filter, find_options).await?;

        let mut big_bang = chrono::offset::Utc::now();
        big_bang = big_bang - Duration::minutes(60);

        while let Some(cm) = cursor.try_next().await? {
            big_bang = cm.last_known_trade_date;
        }
        Ok(big_bang)

    }

    pub fn get_coin(self: &Self) -> Result<&str, NormalError> {
        let v: Vec<&str> = self.market.split("-").collect();
        if v.len() == 1 {
            error!("length of the split is {:?} for {:?} - I will return something but it will NOT match anything", v.len(), v);
            Ok(v[0])
        } else {
            Ok(v[1])
        }
    }

}

impl fmt::Display for CryptoMarket<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10}", self.market)
    }
}





#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Trades<T> {
    pub vts: Vec<T>
}

impl Trades<KafkaCryptoTrade<'_>> {

    pub fn get_total_volume(self: &Self) -> Result<f64, ParseError> {
        let volume: f64 = self.vts.iter().map(|s| s.quantity).sum();
        Ok(volume)
    }

    fn get_vector_of_prices(self: &Self) -> Result<Vec<f64>, ParseError> {
        let vector_of_prices: Vec<f64> = self.vts.iter().map(|s|s.price).collect();
        Ok(vector_of_prices)        
    }
    fn get_vector_of_quantities(self: &Self) -> Result<Vec<f64>, ParseError> {
        let vector_of_quantities: Vec<f64> = self.vts.iter().map(|s|s.quantity).collect();
        Ok(vector_of_quantities)        
    }

    pub fn get_weighted_mean(self: &Self) -> Result<WeightedMean, ParseError> {
        let vector_of_quantities = self.get_vector_of_quantities().unwrap();
        let vector_of_prices = self.get_vector_of_prices().unwrap();
        let wm: WeightedMean = vector_of_prices.clone().iter().zip(vector_of_quantities.clone()).map(|(x,w)| (x.clone(),w.clone())).collect();    
        assert_eq!(wm.sum_weights(), self.get_total_volume().unwrap());
        Ok(wm)
    }

    pub fn get_min_price(self: &Self) -> Result<Min, ParseError> {
        let vector_of_prices = self.get_vector_of_prices().unwrap();
        let min_price: Min = vector_of_prices.iter().collect();
        Ok(min_price)
    }

    pub fn get_max_price(self: &Self) -> Result<Max, ParseError> {
        let vector_of_prices = self.get_vector_of_prices().unwrap();
        let max_price: Max = vector_of_prices.iter().collect();
        Ok(max_price)
    }

    pub fn get_standard_deviation_of_prices(self: &Self) -> Result<Option<f64>, ParseError> {
        let vector_of_prices = self.get_vector_of_prices().unwrap();
        // using a local lib std deviation as rust std libs a little bit of a pain - should change - not this is from affinities process
        let std = std_deviation(&vector_of_prices);
        Ok(std)
    }

    pub fn get_standard_deviation_of_quantities(self: &Self) -> Result<Option<f64>, ParseError> {
        let vector_of_quantities = self.get_vector_of_quantities().unwrap();
        // using a local lib std deviation as rust std libs a little bit of a pain - should change - not this is from affinities process
        let std = std_deviation(&vector_of_quantities);
        Ok(std)
    }

    pub fn get_not_weighted_mean_of_quantities(self: &Self) -> Result<Option<f64>, ParseError> {
        let vector_of_quantities = self.get_vector_of_quantities().unwrap();
        Ok(mean(&vector_of_quantities))
    }

    pub fn get_performance(self: &Self) -> Result<Option<f64>, ParseError> {

        let vector_of_prices = self.get_vector_of_prices().unwrap();
        let perf = if vector_of_prices.len() > 1 {
            let start = vector_of_prices[0];
            let finish = vector_of_prices[vector_of_prices.len()-1];
            Some((finish - start) / start)
        } else {
            None
        };
        Ok(perf)
    }


    pub fn get_wildest(self: &Self) -> Result<Option<f64>, ParseError> {

        let vector_of_prices = self.get_vector_of_prices().unwrap();
        let wildest = if vector_of_prices.len() > 1 {
            let start = self.get_min_price().unwrap().min();
            let finish = self.get_max_price().unwrap().max();
            Some((finish - start) / start)
        } else {
            None
        };

        Ok(wildest)
    }

    pub fn get_last_quantity(self: &Self) -> Result<Option<f64>, ParseError> {
        let vector_of_quantities = self.get_vector_of_quantities().unwrap();
        let last_quantity = if vector_of_quantities.len() > 1 {
            Some(vector_of_quantities[vector_of_quantities.len()-1])
        } else {
            None
        };
        Ok(last_quantity)

    }





}






#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CoinMetrics {
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub last_known_trade_date: DateTime<Utc>,
    pub market: String,
}

impl fmt::Display for CoinMetrics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<35} {:<30}", 
            self.last_known_trade_date, self.market)
    }
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KafkaCryptoTrade<'a> {
    pub trade_date: &'a str,
    pub coin_metrics_id: &'a str,
    pub price: f64,
    pub quantity: f64,
    pub market: &'a str,
    pub tx_type: &'a str,
}

impl KafkaCryptoTrade<'_> {

    pub fn get_net(self: &Self) -> Result<f64, ParseError> {
        Ok(&self.price * &self.quantity)
    }

}


impl fmt::Display for KafkaCryptoTrade<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<35} {:>6.2} {:>9.4} {:<30} {:<5}", 
            &self.coin_metrics_id, &self.trade_date, self.price, self.quantity, &self.market, &self.tx_type)
    }
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



// #[derive(Debug, Serialize, Deserialize, Clone)]
// pub struct AnalysisArtifact {
//     #[serde(with = "chrono_datetime_as_bson_datetime")]
//     pub ltdate: DateTime<Utc>,
//     #[serde(rename = "symbol", default)]
//     pub symbol: Option<String>,
// }

// impl AnalysisArtifact {

//     pub async fn get_history(self: &Self, lb: &i64, collection: &Collection<TLPhemexMDSnapshot>) -> Result<Vec<TLPhemexMDSnapshot>, Error> {

//         let gtedate = self.ltdate - Duration::milliseconds(*lb);

//         let mut crts: Vec<TLPhemexMDSnapshot> = Vec::new();
//         let filter = doc! {"snapshot_date": {"$gte": gtedate}, "symbol": self.symbol.as_ref()};
//         let find_options = FindOptions::builder().sort(doc! { "snapshot_date":1}).build();
//         let mut cursor = collection.find(filter, find_options).await?;
//         while let Some(crt) = cursor.try_next().await? {
//             crts.push(crt.clone());                
//         }
//         Ok(crts)
//     }

//     pub async fn get_open_interest_delta(self: &Self, lb: &i64, collection: &Collection<TLPhemexMDSnapshot>) -> Result<f64, Error> {
//         let h = self.get_history(&lb, &collection).await.unwrap();
//         let d = (h[h.len()-1].open_interest as f64 - h[0].open_interest as f64) / h[0].open_interest as f64;
//         Ok(d*100.00)
//     }

//     pub async fn get_mark_price_delta(self: &Self, lb: &i64, collection: &Collection<TLPhemexMDSnapshot>) -> Result<f64, Error> {
//         let h = self.get_history(&lb, &collection).await.unwrap();
//         let d = (h[h.len()-1].mark_price as f64 - h[0].mark_price as f64) / h[0].mark_price as f64;
//         Ok(d*100.00)
//     }



// }




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




pub struct TLDYDXMarket<'a> {
    pub market: &'a str,
    pub status: &'a str,
    pub base_asset: &'a str,
    pub quote_asset: &'a str,
    pub step_size: f64,
    pub tick_size: f64,
    pub index_price: f64,
    pub oracle_price: f64
}

impl fmt::Display for TLDYDXMarket<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<10} {:>10} {:>10} {:>10.4} {:>10.4} {:>10.4} {:>10.4}", self.market, self.status, self.base_asset, self.quote_asset, self.step_size, self.tick_size, self.index_price, self.oracle_price)
    }
}



#[derive(Deserialize, Debug)]
pub struct DYDXMarket {
    pub market: String,
    pub status: String,
    #[serde(rename(deserialize = "baseAsset"))]
    pub base_asset: String,
    #[serde(rename(deserialize = "quoteAsset"))]
    pub quote_asset: String,
    #[serde(rename(deserialize = "stepSize"))]
    pub step_size: String,
    #[serde(rename(deserialize = "tickSize"))]
    pub tick_size: String,
    #[serde(rename(deserialize = "indexPrice"))]
    pub index_price: String,
    #[serde(rename(deserialize = "oraclePrice"))]
    pub oracle_price: String
}

impl DYDXMarket {
    pub fn get_tl_version(self: &Self) -> Result<TLDYDXMarket, NormalError> {
        Ok(TLDYDXMarket {
            market: &self.market,
            status: &self.status,
            base_asset: &self.base_asset,
            quote_asset: &self.quote_asset,
            step_size: self.step_size.parse::<f64>().unwrap(),
            tick_size: self.tick_size.parse::<f64>().unwrap(),
            index_price: self.index_price.parse::<f64>().unwrap(),
            oracle_price: self.oracle_price.parse::<f64>().unwrap()
        })
    }

}

impl fmt::Display for DYDXMarket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}", self.market, self.status, self.base_asset, self.quote_asset, self.step_size, self.tick_size, self.index_price, self.oracle_price)
    }
}


#[derive(Deserialize, Debug)]
pub struct DYDXMarkets {
    #[serde(rename(deserialize = "markets"))]
    pub markets: HashMap<String,DYDXMarket>
}





    














#[derive(Debug, Clone)]
pub struct TimeRange {
    pub gtedate: DateTime<Utc>,
    pub ltdate: DateTime<Utc>,
}

impl TimeRange {

    pub fn get_hourlies(self: &Self) -> Result<Vec<TimeRange>,Error> {
        let mut time_ranges = Vec::new();
        let mut dt = self.gtedate;
        while dt < self.ltdate {
            let gtd = dt;
            let ltd = gtd + Duration::hours(1);
            dt = dt + Duration::hours(1);

            let tr = TimeRange {
                gtedate: gtd.clone(),
                ltdate: ltd.clone(),
            };
            time_ranges.push(tr);
        }
        Ok(time_ranges)
    }

    pub async fn delete_exact_range<T>(self: &Self, collection: &Collection<T>) -> Result<(),Error> {
        collection.delete_many(doc!{"gtedate": &self.gtedate, "ltdate": &self.ltdate}, None).await?;    
        debug!("deleted {} {}", &self.gtedate, &self.ltdate);
        Ok(())
    }

}


