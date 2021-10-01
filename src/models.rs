// this use statement gets you access to the lib file - you use crate instead of the package name, who the fuck knows why (see any bin rs)
use crate::*;

use serde::{Serialize,Deserialize};
use bson::serde_helpers::chrono_datetime_as_bson_datetime;
use chrono::{DateTime,Utc,TimeZone,SecondsFormat};
use chrono::format::ParseError;

use mongodb::{Collection};
use mongodb::{error::Error};
use mongodb::{bson::doc};
use mongodb::options::{FindOptions};
use futures::stream::TryStreamExt;

use time::Duration;
use average::{WeightedMean,Min,Max};


use std::fmt; // Import `fmt`



#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CryptoAsset {
    pub asset: String,
}

impl CryptoAsset {

    pub async fn get_last_updated_cap_suite(self: &Self, collection: &Collection<CapSuite>) -> Result<DateTime<Utc>, Error> {

        let filter = doc! {"asset": &self.asset};
        let find_options = FindOptions::builder().sort(doc! { "trade_date":-1}).limit(1).build();
        let mut cursor = collection.find(filter, find_options).await?;

        let mut big_bang = Utc.ymd(1970, 1, 1).and_hms_nano(0, 0, 1, 444);

        while let Some(trade) = cursor.try_next().await? {
            big_bang = trade.trade_date;
        }
        Ok(big_bang)

    }
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CapSuite {
    pub asset: String,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub trade_date: DateTime<Utc>,
    pub price: f64,
    pub cap_mvrv_cur: f64,
    pub cap_mvrv_ff: f64,
    pub cap_mrkt_cur_usd: f64,
    pub cap_mrkt_ff_usd: f64,
    pub cap_real_usd: f64,
}

impl CapSuite {

    pub fn get_csv(self: &Self) -> Result<CapSuiteCSV,ParseError> {
        Ok(CapSuiteCSV {
            asset: self.asset.clone(),
            trade_date: self.trade_date.to_rfc3339(),
            price: self.price,
            cap_mvrv_cur: self.cap_mvrv_cur,
            cap_mvrv_ff: self.cap_mvrv_ff,
            cap_mrkt_cur_usd: self.cap_mrkt_cur_usd,
            cap_mrkt_ff_usd: self.cap_mrkt_ff_usd,
            cap_real_usd: self.cap_real_usd
        })
    }

}

impl fmt::Display for CapSuite {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:>5} {:<9} {:>12.4} {:>12.4} {:>12.4} {:>12.4} {:>12.4} {:>12.4}", 
            self.asset, self.trade_date, self.price, self.cap_mvrv_cur, self.cap_mvrv_ff, self.cap_mrkt_cur_usd, self.cap_mrkt_ff_usd, self.cap_real_usd)
    }
}


#[derive(Debug, Serialize)]
pub struct CapSuiteCSV {
    #[serde(rename(serialize = "Asset"))]
    asset: String,
    #[serde(rename(serialize = "TradeDate"))]
    pub trade_date: String,
    #[serde(rename(serialize = "Price"))]
    price: f64,
    #[serde(rename(serialize = "CapMVRVCur"))]
    cap_mvrv_cur: f64,
    #[serde(rename(serialize = "CapMVRVFF"))]
    cap_mvrv_ff: f64,
    #[serde(rename(serialize = "CapMrktCurUSD"))]
    cap_mrkt_cur_usd: f64,
    #[serde(rename(serialize = "CapMrktFFUSD"))]
    cap_mrkt_ff_usd: f64,
    #[serde(rename(serialize = "CapRealUSD"))]
    cap_real_usd: f64,
}








#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CryptoMarket {
    pub market: String,
}

impl CryptoMarket {


    pub fn drop_all_but_instrument_type(self: &Self) -> Result<&str, ParseError> {
        let market_array = self.market.split('-').collect::<Vec<&str>>();
        Ok(market_array[market_array.len()-1])
    }

    pub fn drop_exchange(self: &Self) -> Result<String, ParseError> {
        let mut market_array = self.market.split('-').collect::<Vec<&str>>();
        market_array.remove(0);
        Ok(market_array.join("-"))
    }

    pub fn just_exchange(self: &Self) -> Result<&str, ParseError> {
        let market_array = self.market.split('-').collect::<Vec<&str>>();
        Ok(market_array[0])
    }


    pub async fn get_last_updated_trade(self: &Self, collection: &Collection<CryptoTrade>) -> Result<DateTime<Utc>, Error> {

        let filter = doc! {"market": &self.market};
        let find_options = FindOptions::builder().sort(doc! { "trade_date":-1}).limit(1).build();
        let mut cursor = collection.find(filter, find_options).await?;

        let mut big_bang = Utc.ymd(1970, 1, 1).and_hms_nano(0, 0, 1, 444);

        while let Some(trade) = cursor.try_next().await? {
            big_bang = trade.trade_date;
        }
        Ok(big_bang)

    }

    pub async fn get_last_updated_liquidation(self: &Self, collection: &Collection<CryptoLiquidation>) -> Result<DateTime<Utc>, Error> {

        let filter = doc! {"market": &self.market};
        let find_options = FindOptions::builder().sort(doc! { "trade_date":-1}).limit(1).build();
        let mut cursor = collection.find(filter, find_options).await?;

        let mut big_bang = Utc.ymd(1970, 1, 1).and_hms_nano(0, 0, 1, 444);

        while let Some(trade) = cursor.try_next().await? {
            big_bang = trade.trade_date;
        }
        Ok(big_bang)

    }

}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CryptoLiquidation {
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub trade_date: DateTime<Utc>,
    pub coin_metrics_id: String,
    pub price: f64,
    pub quantity: f64,
    pub market: String,
    pub tx_type: String,
    pub cm_type: String,
    pub trade_llama_exchange: String,
    pub trade_llama_instrument: String,
    pub trade_llama_instrument_type: String,
}

impl CryptoLiquidation {

    pub fn get_net(self: &Self) -> Result<f64, ParseError> {
        Ok(&self.price * &self.quantity)
    }

    pub async fn get_comparables(self: &Self, lb: &i64, la: &i64, collection: &Collection<CryptoTrade>) -> Result<Vec<CryptoTrade>, Error> {

        let gtedate = self.trade_date - Duration::milliseconds(*lb);
        let ltdate = self.trade_date + Duration::milliseconds(*la);

        let mut crts: Vec<CryptoTrade> = Vec::new();
        let filter = doc! {"trade_date": {"$gte": gtedate, "$lt": ltdate}, "tx_type": &self.tx_type, "market": &self.market};
        let find_options = FindOptions::builder().sort(doc! { "trade_date":1}).build();
        let mut cursor = collection.find(filter, find_options).await?;
        while let Some(crt) = cursor.try_next().await? {
            crts.push(crt.clone());                
        }

        Ok(crts)

    }


    pub async fn get_comparable_spots(self: &Self, lb: &i64, la: &i64, collection: &Collection<CryptoTrade>) -> Result<Vec<CryptoTrade>, Error> {

        let gtedate = self.trade_date - Duration::milliseconds(*lb);
        let ltdate = self.trade_date + Duration::milliseconds(*la);

        let mut crts: Vec<CryptoTrade> = Vec::new();
        let filter = doc! {"trade_date": {"$gte": gtedate, "$lt": ltdate}, "tx_type": &self.tx_type, "trade_llama_instrument_type": "spot"};
        let find_options = FindOptions::builder().sort(doc! { "trade_date":1}).build();
        let mut cursor = collection.find(filter, find_options).await?;
        while let Some(crt) = cursor.try_next().await? {
            crts.push(crt.clone());                
        }

        Ok(crts)

    }


}

impl fmt::Display for CryptoLiquidation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<9} {:<9} {:>6.2} {:>9.4} {:<30} {:<5} {:<5} {:<30} {:<30} {:<30}", 
            &self.coin_metrics_id, &self.trade_date, self.price, self.quantity, &self.market, &self.tx_type, &self.cm_type,
            &self.trade_llama_exchange, &self.trade_llama_instrument, &self.trade_llama_instrument_type)
    }
}






#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Trades<T> {
    pub vts: Vec<T>
}

impl Trades<CryptoTrade> {

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



impl Trades<CryptoLiquidation> {

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

    fn get_vector_of_prices_and_quantities(self: &Self) -> Result<Vec<f64>, ParseError> {
        let vp = self.get_vector_of_prices().unwrap();
        let vq = self.get_vector_of_quantities().unwrap();
        let mut rvec = Vec::new();
        for (idx, p) in vp.iter().enumerate() {
            rvec.push(*p);
            rvec.push(vq[idx]);
        }
        Ok(rvec)
    }

    pub fn get_pqkm(self: &Self) -> Result<(Vec<f64>,Vec<f64>,Vec<f64>,Vec<i32>), Error> {
        let p = self.get_vector_of_prices().unwrap();
        let q = self.get_vector_of_quantities().unwrap();
        let pq = self.get_vector_of_prices_and_quantities().unwrap();
        let rkm = do_duo_kmeans(&pq);
        Ok((p,q,pq,rkm))
    }



    pub fn get_weighted_mean(self: &Self) -> Result<WeightedMean, ParseError> {
        let vector_of_quantities = self.get_vector_of_quantities().unwrap();
        let vector_of_prices = self.get_vector_of_prices().unwrap();
        let wm: WeightedMean = vector_of_prices.clone().iter().zip(vector_of_quantities.clone()).map(|(x,w)| (x.clone(),w.clone())).collect();    
        assert_eq!(wm.sum_weights(), self.get_total_volume().unwrap());
        Ok(wm)
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


}












#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CryptoTrade {
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub trade_date: DateTime<Utc>,
    pub coin_metrics_id: String,
    pub price: f64,
    pub quantity: f64,
    pub market: String,
    pub tx_type: String,
    pub trade_llama_exchange: String,
    pub trade_llama_instrument: String,
    pub trade_llama_instrument_type: String,
}
impl CryptoTrade {

    pub fn get_net(self: &Self) -> Result<f64, ParseError> {
        Ok(&self.price * &self.quantity)
    }

}


impl fmt::Display for CryptoTrade {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<9} {:<9} {:>6.2} {:>9.4} {:<30} {:<5} {:<30} {:<30} {:<30}", 
            &self.coin_metrics_id, &self.trade_date, self.price, self.quantity, &self.market, &self.tx_type, 
            &self.trade_llama_exchange, &self.trade_llama_instrument, &self.trade_llama_instrument_type)
    }
}













#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MarketSummary {
   pub _id: String,
   pub cnt: f64,
   pub qty: f64,
   pub std: f64,
   pub na: f64,
}

impl fmt::Display for MarketSummary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<30} {:>9.2} {:>9.2} {:>9.2} {:>9.2}", &self._id, &self.cnt, &self.qty, &self.std, &self.na)
    }
}



#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RangeBoundMarketSummary {
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub gtedate: DateTime<Utc>,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub ltdate: DateTime<Utc>,    
    pub description: String,
    pub market_summary: MarketSummary
}

impl RangeBoundMarketSummary {
    pub fn get_csv(self: &Self) -> Result<RangeBoundMarketSummaryCSV,ParseError> {
        Ok(RangeBoundMarketSummaryCSV {
            gtedate: self.gtedate.to_rfc3339_opts(SecondsFormat::Secs, true),
            ltdate: self.ltdate.to_rfc3339(),
            description: self.description.clone(),
            market: self.market_summary._id.clone(),
            cnt: self.market_summary.cnt,
            qty: self.market_summary.qty,
            std: self.market_summary.std,
            na: self.market_summary.na 
        })
    }

}

impl fmt::Display for RangeBoundMarketSummary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<30} {:<30} {:<30} {:<40} {:>9.2} {:>9.2} {:>9.2} {:>9.2}", &self.gtedate, &self.ltdate, &self.description, &self.market_summary._id, &self.market_summary.cnt, &self.market_summary.qty, &self.market_summary.std, &self.market_summary.na)
    }
}


#[derive(Debug, Serialize)]
pub struct RangeBoundMarketSummaryCSV {
    #[serde(rename(serialize = "GTEDate"))]
    gtedate: String,
    #[serde(rename(serialize = "LTDate"))]
    ltdate: String,
    #[serde(rename(serialize = "Description"))]
    description: String,
    #[serde(rename(serialize = "Market"))]
    market: String,
    #[serde(rename(serialize = "CNT"))]
    cnt: f64,
    #[serde(rename(serialize = "QTY"))]
    qty: f64,
    #[serde(rename(serialize = "STD"))]
    std: f64,
    #[serde(rename(serialize = "NA"))]
    na: f64,
}



#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExchangeSummaryKey {
   pub trade_llama_exchange: String,
   pub trade_llama_instrument_type: String,
   pub tx_type: String,
}

impl fmt::Display for ExchangeSummaryKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<30} {:<30} {:<30}", &self.trade_llama_exchange, &self.trade_llama_instrument_type, &self.tx_type)
    }
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExchangeSummary {
   pub _id: ExchangeSummaryKey,
   pub cnt: f64,
   pub na: f64,
}

impl fmt::Display for ExchangeSummary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<95} {:>9.2} {:>9.2}", &self._id, &self.cnt, &self.na)
    }
}



#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RangeBoundExchangeSummary {
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub gtedate: DateTime<Utc>,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub ltdate: DateTime<Utc>,    
    pub description: String,
    pub exchange_summary: ExchangeSummary
}

impl RangeBoundExchangeSummary {
    pub fn get_csv(self: &Self) -> Result<RangeBoundExchangeSummaryCSV,ParseError> {
        Ok(RangeBoundExchangeSummaryCSV {
            gtedate: self.gtedate.to_rfc3339_opts(SecondsFormat::Secs, true),
            ltdate: self.ltdate.to_rfc3339(),
            description: self.description.clone(),
            trade_llama_exchange: self.exchange_summary._id.trade_llama_exchange.clone(),
            trade_llama_instrument_type: self.exchange_summary._id.trade_llama_instrument_type.clone(),
            tx_type: self.exchange_summary._id.tx_type.clone(),
            cnt: self.exchange_summary.cnt,
            na: self.exchange_summary.na 
        })
    }

}

impl fmt::Display for RangeBoundExchangeSummary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<30} {:<30} {:<30} {:<40} {:>9.2} {:>9.2}", &self.gtedate, &self.ltdate, &self.description, &self.exchange_summary._id, &self.exchange_summary.cnt, &self.exchange_summary.na)
    }
}


#[derive(Debug, Serialize)]
pub struct RangeBoundExchangeSummaryCSV {
    #[serde(rename(serialize = "GTEDate"))]
    gtedate: String,
    #[serde(rename(serialize = "LTDate"))]
    ltdate: String,
    #[serde(rename(serialize = "Description"))]
    description: String,
    #[serde(rename(serialize = "Exchange"))]
    trade_llama_exchange: String,
    #[serde(rename(serialize = "InstrumentType"))]
    trade_llama_instrument_type: String,
    #[serde(rename(serialize = "TXType"))]
    tx_type: String,
    #[serde(rename(serialize = "CNT"))]
    cnt: f64,
    #[serde(rename(serialize = "NA"))]
    na: f64,
}





#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RangeBoundLiquidationCluster {
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub gtedate: DateTime<Utc>,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub ltdate: DateTime<Utc>,   
    pub tx_type: String, 
    pub price: f64,
    pub quantity: f64,
    pub cluster: i32
}

impl RangeBoundLiquidationCluster {
    pub fn get_csv(self: &Self) -> Result<RangeBoundLiquidationClusterCSV,ParseError> {
        Ok(RangeBoundLiquidationClusterCSV {
            gtedate: self.gtedate.to_rfc3339(),
            ltdate: self.ltdate.to_rfc3339(),
            tx_type: self.tx_type.clone(),
            price: self.price,
            quantity: self.quantity,
            cluster: self.cluster 
        })
    }

}



impl fmt::Display for RangeBoundLiquidationCluster {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<30} {:<30} {:<10} {:>9.2} {:>9.4} {:>4}", &self.gtedate, &self.ltdate, &self.tx_type, &self.price, &self.quantity, &self.cluster)
    }
}


#[derive(Debug, Serialize)]
pub struct RangeBoundLiquidationClusterCSV {
    #[serde(rename(serialize = "GTEDate"))]
    gtedate: String,
    #[serde(rename(serialize = "LTDate"))]
    ltdate: String,
    #[serde(rename(serialize = "TXType"))]
    tx_type: String,
    #[serde(rename(serialize = "Price"))]
    price: f64,
    #[serde(rename(serialize = "Quantity"))]
    quantity: f64,
    #[serde(rename(serialize = "Cluster"))]
    cluster: i32,
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


