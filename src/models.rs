// this use statement gets you access to the lib file - you use crate instead of the package name, who the fuck knows why (see any bin rs)

use crate::*;

use std::collections::HashMap;


use serde::{Serialize,Deserialize};
use bson::serde_helpers::chrono_datetime_as_bson_datetime;
use chrono::{DateTime,Utc};
use chrono::format::ParseError;

use mongodb::{Collection};
use mongodb::error::Error as MongoError;
use mongodb::{bson::doc};
use mongodb::options::{FindOptions};
use futures::stream::TryStreamExt;

use time::Duration;
use average::{WeightedMean,Min,Max};


use std::fmt; // Import `fmt`
use std::fmt::Error as NormalError;



#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterBomb<'a> {
    pub market: &'a str,
    pub min_date: &'a str,
    pub max_date: &'a str,
    pub minutes: i64,
    pub interval_return: f64,
    pub interval_std: f64,
    pub float_one: f64,
    pub float_two: f64,
    pub group: i32
}



impl fmt::Display for ClusterBomb<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<10} {:<10} {:<10} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>2}", 
            self.market, self.min_date, self.max_date, self.minutes, self.interval_return, self.interval_std, self.float_one, self.float_two, self.group)
    }   
}
 
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterBombTriple<'a> {
    pub market: &'a str,
    pub min_date: &'a str,
    pub max_date: &'a str,
    pub minutes: i64,
    pub interval_return: f64,
    pub interval_std: f64,
    pub float_one: f64,
    pub float_two: f64,
    pub float_three: f64,
    pub group: i32
}



impl fmt::Display for ClusterBombTriple<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<10} {:<10} {:<10} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>2}", 
            self.market, self.min_date, self.max_date, self.minutes, self.interval_return, self.interval_std, self.float_one, self.float_two, self.float_three, self.group)
    }   
}
 


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterBombTripleBonused<'a> {
    pub market: &'a str,
    pub min_date: &'a str,
    pub max_date: &'a str,
    pub minutes: i64,
    pub interval_return: f64,
    pub interval_std: f64,
    pub float_one: f64,
    pub float_two: f64,
    pub float_three: f64,
    pub group: i32,
    pub tl_derived_price_change_1m: f64,
    pub tl_derived_open_interest_change_10m: f64,
    pub mongo_snapshot_date: &'a str
}



impl fmt::Display for ClusterBombTripleBonused<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<10} {:<10} {:<10} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>2} {:>10.4}", 
            self.market, self.min_date, self.max_date, self.minutes, self.interval_return, self.interval_std, self.float_one, self.float_two, self.float_three, self.group, self.tl_derived_price_change_10m)
    }   
}




#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AssetPair<'a> {
    pub altname: &'a str,
    pub base_asset: &'a str,
    pub quote_asset: &'a str,
}

impl AssetPair<'_> {

    pub async fn get_last_updated_trade<'a>(self: &Self, source: &'a str, collection: &Collection<SourceThingLastUpdate>) -> Result<DateTime<Utc>, MongoError> {

        let filter = doc! {"thing": &self.altname, "source": source, "thing": "asset pair"};
        let find_options = FindOptions::builder().sort(doc! { "trade_date":-1}).limit(1).build();
        let mut cursor = collection.find(filter, find_options).await?;

        let mut big_bang = chrono::offset::Utc::now();
        big_bang = big_bang - Duration::minutes(60);

        while let Some(cm) = cursor.try_next().await? {
            big_bang = cm.last_known_trade_date;
        }
        Ok(big_bang)

    }


}

impl fmt::Display for AssetPair<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<10} {:<10}", self.altname, self.base_asset, self.quote_asset)
    }
}



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

    pub fn is_kraken(self: &Self) -> bool {
        match self.get_exchange().unwrap().to_lowercase().as_ref() {
            "kraken" => true,
            _ => false
        } 
    }

    pub async fn get_last_updated_trade(self: &Self, collection: &Collection<SourceThingLastUpdate>) -> Result<DateTime<Utc>, MongoError> {

        let filter = doc! {"thing": &self.market, "source": "coinmetrics", "thing": "market"};
        let find_options = FindOptions::builder().sort(doc! { "trade_date":-1}).limit(1).build();
        let mut cursor = collection.find(filter, find_options).await?;

        let mut big_bang = chrono::offset::Utc::now();
        big_bang = big_bang - Duration::minutes(60);

        while let Some(cm) = cursor.try_next().await? {
            big_bang = cm.last_known_trade_date;
        }
        Ok(big_bang)

    }

    pub async fn get_last_migrated_trade(self: &Self, collection: &Collection<CryptoTrade>) -> Result<DateTime<Utc>, MongoError> {

        let filter = doc! {"market": &self.market};
        let find_options = FindOptions::builder().sort(doc! { "trade_date":-1}).limit(1).build();
        let mut cursor = collection.find(filter, find_options).await?;

        let mut big_bang = chrono::offset::Utc::now();
        // we go back a lot farther for the trade stuff cause there's no limit
        big_bang = big_bang - Duration::minutes(600000);

        while let Some(cm) = cursor.try_next().await? {
            big_bang = cm.trade_date;
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

    pub fn get_exchange(self: &Self) -> Result<&str, NormalError> {
        let v: Vec<&str> = self.market.split("-").collect();
        Ok(v[0])
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
pub struct SourceThingLastUpdate {
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub last_known_trade_date: DateTime<Utc>,
    pub source: String,
    pub thing: String,
    pub thing_description: String
}

impl fmt::Display for SourceThingLastUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<35} {:<30} {:<30} {:<30}", 
            self.last_known_trade_date, self.source, self.thing, self.thing_description)
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

    pub fn get_crypto_trade_for_mongo(self: &Self) -> Result<CryptoTrade, ParseError> {
        let trade_date = DateTime::parse_from_rfc3339(&self.trade_date).unwrap();
        Ok(CryptoTrade {
            trade_date: trade_date.with_timezone(&Utc),
            coin_metrics_id: self.coin_metrics_id.to_owned(),
            price: self.price,
            quantity: self.quantity,
            market: self.market.to_owned(),
            tx_type: self.tx_type.to_owned()
        })
    }

}


impl fmt::Display for KafkaCryptoTrade<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<35} {:>6.2} {:>9.4} {:<30} {:<5}", 
            &self.coin_metrics_id, &self.trade_date, self.price, self.quantity, &self.market, &self.tx_type)
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
}

impl CryptoTrade {

    pub fn get_net(self: &Self) -> Result<f64, ParseError> {
        Ok(&self.price * &self.quantity)
    }

}

impl fmt::Display for CryptoTrade {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<35} {:>6.2} {:>9.4} {:<30} {:<5}", 
            &self.coin_metrics_id, &self.trade_date, self.price, self.quantity, &self.market, &self.tx_type)
    }
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






// #[derive(Debug, Serialize, Deserialize, Clone)]
// pub struct TLDYDXMarket<'a> {
//     pub snapshot_date: &'a str,
//     pub market: &'a str,
//     pub status: &'a str,
//     pub base_asset: &'a str,
//     pub quote_asset: &'a str,
//     pub step_size: f64,
//     pub tick_size: f64,
//     pub index_price: f64,
//     pub oracle_price: f64,
//     pub tl_derived_index_oracle_spread: f64,
//     pub price_change_24h: f64,
//     pub next_funding_rate: f64,
//     pub next_funding_at: &'a str,
//     pub min_order_size: f64,
//     pub instrument_type: &'a str,
//     pub initial_margin_fraction: f64,
//     pub maintenance_margin_fraction: f64,
//     pub baseline_position_size: f64,
//     pub incremental_position_size: f64,
//     pub incremental_initial_margin_fraction: f64,
//     pub volume_24h: f64,
//     pub trades_24h: f64,
//     pub open_interest: f64,
//     pub max_position_size: f64,
//     pub asset_resolution: f64,
// }


// impl fmt::Display for TLDYDXMarket<'_> {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "{:<10} {:<10} {:<10} {:>10} {:>10} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10} {:>10.4} {:>10} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4}", self.snapshot_date, self.market, self.status, self.base_asset, self.quote_asset, self.step_size, self.tick_size, self.index_price, self.oracle_price, self.tl_derived_index_oracle_spread, self.price_change_24h, self.next_funding_rate, self.next_funding_at, self.min_order_size, self.instrument_type, self.initial_margin_fraction, self.maintenance_margin_fraction, self.baseline_position_size, self.incremental_position_size, self.incremental_initial_margin_fraction, self.volume_24h, self.trades_24h, self.open_interest, self.max_position_size, self.asset_resolution)
//     }
// }


// #[derive(Deserialize, Debug)]
// pub struct DYDXMarket {
//     pub market: String,
//     pub status: String,
//     #[serde(rename(deserialize = "baseAsset"))]
//     pub base_asset: String,
//     #[serde(rename(deserialize = "quoteAsset"))]
//     pub quote_asset: String,
//     #[serde(rename(deserialize = "stepSize"))]
//     pub step_size: String,
//     #[serde(rename(deserialize = "tickSize"))]
//     pub tick_size: String,
//     #[serde(rename(deserialize = "indexPrice"))]
//     pub index_price: String,
//     #[serde(rename(deserialize = "oraclePrice"))]
//     pub oracle_price: String,
//     #[serde(rename(deserialize = "priceChange24H"))]
//     pub price_change_24h: String,
//     #[serde(rename(deserialize = "nextFundingRate"))]
//     pub next_funding_rate: String,
//     #[serde(rename(deserialize = "nextFundingAt"))]
//     pub next_funding_at: String,
//     #[serde(rename(deserialize = "minOrderSize"))]
//     pub min_order_size: String,
//     #[serde(rename(deserialize = "type"))]
//     pub instrument_type: String,
//     #[serde(rename(deserialize = "initialMarginFraction"))]
//     pub initial_margin_fraction: String,
//     #[serde(rename(deserialize = "maintenanceMarginFraction"))]
//     pub maintenance_margin_fraction: String,
//     #[serde(rename(deserialize = "baselinePositionSize"))]
//     pub baseline_position_size: String,
//     #[serde(rename(deserialize = "incrementalPositionSize"))]
//     pub incremental_position_size: String,
//     #[serde(rename(deserialize = "incrementalInitialMarginFraction"))]
//     pub incremental_initial_margin_fraction: String,
//     #[serde(rename(deserialize = "volume24H"))]
//     pub volume_24h: String,
//     #[serde(rename(deserialize = "trades24H"))]
//     pub trades_24h: String,
//     #[serde(rename(deserialize = "openInterest"))]
//     pub open_interest: String,
//     #[serde(rename(deserialize = "maxPositionSize"))]
//     pub max_position_size: String,
//     #[serde(rename(deserialize = "assetResolution"))]
//     pub asset_resolution: String,

// }


// impl fmt::Display for DYDXMarket {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "{:<10} {:<10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}", self.market, self.status, self.base_asset, self.quote_asset, self.step_size, self.tick_size, self.index_price, self.oracle_price, self.price_change_24h, self.next_funding_rate, self.next_funding_at, self.min_order_size, self.instrument_type, self.initial_margin_fraction, self.maintenance_margin_fraction, self.baseline_position_size, self.incremental_position_size, self.incremental_initial_margin_fraction, self.volume_24h, self.trades_24h, self.open_interest, self.max_position_size, self.asset_resolution)
//     }
// }


// #[derive(Deserialize, Debug)]
// pub struct DYDXMarkets {
//     #[serde(rename(deserialize = "markets"))]
//     pub markets: HashMap<String,DYDXMarket>
// }





    


#[derive(Deserialize, Debug)]
pub struct KrakenAssetPair {
    pub altname: String,
    pub wsname: String,
    pub aclass_base: String,
    pub base: String,
    pub aclass_quote: String,
    pub quote: String,
    pub lot: String,
    pub pair_decimals: i32,
    pub lot_decimals: i32,
    pub lot_multiplier: i32
}

impl fmt::Display for KrakenAssetPair {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, " {:>10}", self.altname)
    }
}

#[derive(Deserialize, Debug)]
pub struct KrakenAssetPairs {
    #[serde(rename(deserialize = "result"))]
    pub asset_pairs: HashMap<String,KrakenAssetPair>
}


#[derive(Deserialize, Debug)]
pub struct KrakenAsset {
    pub aclass: String,
    pub altname: String,
    pub decimals: i32,
    pub display_decimals: i32
}

impl fmt::Display for KrakenAsset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:>10} {:>10} {:>10} {:>10}", self.aclass, self.altname, self.decimals, self.display_decimals)
    }
}

#[derive(Deserialize, Debug)]
pub struct KrakenAssets {
    #[serde(rename(deserialize = "result"))]
    pub assets: HashMap<String,KrakenAsset>
}


// unfortunately, I could never get this to serde deserialize magic, so we use the serde_json elsewhere, and 
// build a normal struct
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KafkaKrakenTrade<'a> {
    pub asset_pair: &'a str,
    pub price: f64,
    pub quantity: f64,
    pub trade_date: &'a str,
    pub tx_type: &'a str,
    pub order_type: &'a str,
    pub some_other_thing: Option<&'a str>
}

impl fmt::Display for KafkaKrakenTrade<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:>10} {:>10.4} {:>10.4} {:>10} {:>10} {:>10} {:>10}", self.asset_pair, self.price, self.quantity, self.trade_date, self.tx_type, self.order_type, self.some_other_thing.unwrap_or(""))
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

impl fmt::Display for RangeBoundMarketSummary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<30} {:<30} {:<30} {:<40} {:>9.2} {:>9.2} {:>9.2} {:>9.2}", &self.gtedate, &self.ltdate, &self.description, &self.market_summary._id, &self.market_summary.cnt, &self.market_summary.qty, &self.market_summary.std, &self.market_summary.na)
    }
}




// /// Very useful - set a begin and end, and have generic collections for calls in the methods.  Note that you get away with the complete generic on collection, because not finding (not needing any data parm knowledge).
// /// So all methods need to be very grandiose, like delete all.
// #[derive(Debug, Clone)]
// pub struct TimeRange {
//     pub gtedate: DateTime<Utc>,
//     pub ltdate: DateTime<Utc>,
// }

// impl TimeRange {

//     /// So you give it a span, say a day, in the struct, and it returns a vec of same struct, hourly intervals.  Great to feed into a multithread where you want to process hourlies in tandem for a day.
//     pub fn get_hourlies(self: &Self) -> Result<Vec<TimeRange>,Box<dyn Error>> {
//         let mut time_ranges = Vec::new();
//         let mut dt = self.gtedate;
//         while dt < self.ltdate {
//             let gtd = dt;
//             let ltd = gtd + Duration::hours(1);
//             dt = dt + Duration::hours(1);

//             let tr = TimeRange {
//                 gtedate: gtd.clone(),
//                 ltdate: ltd.clone(),
//             };
//             time_ranges.push(tr);
//         }
//         Ok(time_ranges)
//     }

//     /// Pass any collection, delete the range.  Delete many works with generic collection, weird.  I don't think a find op would.
//     pub async fn delete_exact_range<T>(self: &Self, collection: &Collection<T>) -> Result<(),MongoError> {
//         collection.delete_many(doc!{"gtedate": &self.gtedate, "ltdate": &self.ltdate}, None).await?;    
//         debug!("deleted {} {}", &self.gtedate, &self.ltdate);
//         Ok(())
//     }

//     ///This is used to fetch a range count per asset pair.  It's primary use is to identify when the quote process stalled, and how long it was out for.
//     ///Note that it is specifc to TLDYDXMarket struct.
//     pub async fn range_count<'a>(self: &Self, dydxcol: &Collection<TLDYDXMarket>) -> Result<HashMap<String,i32>, MongoError> {
//         let filter = doc! {"mongo_snapshot_date": {"$gte": self.gtedate}, "mongo_snapshot_date": {"$lt": self.ltdate}};
//         let find_options = FindOptions::builder().sort(doc! { "mongo_snapshot_date":1}).build();
//         let mut cursor = dydxcol.find(filter, find_options).await?;
//         let mut hm: HashMap<String, i32> = HashMap::new(); 
//         while let Some(des_tldm) = cursor.try_next().await? {
//             hm.entry(des_tldm.market.clone()).and_modify(|e| { *e += 1}).or_insert(1);
//         }
//         Ok(hm)
//     }




// }


