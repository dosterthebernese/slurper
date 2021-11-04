//use crate::*;

use std::collections::HashMap;
use serde::{Serialize,Deserialize};
use mongodb::{bson::doc};
use std::fmt; // Import `fmt`


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TLDYDXMarket<'a> {
    pub snapshot_date: &'a str,
    pub market: &'a str,
    pub status: &'a str,
    pub base_asset: &'a str,
    pub quote_asset: &'a str,
    pub step_size: f64,
    pub tick_size: f64,
    pub index_price: f64,
    pub oracle_price: f64,
    pub tl_derived_index_oracle_spread: f64,
    pub price_change_24h: f64,
    pub tl_derived_price_change_5s: Option<f64>,
    pub tl_derived_price_change_10s: Option<f64>,
    pub tl_derived_price_change_30s: Option<f64>,
    pub tl_derived_price_change_1m: Option<f64>,
    pub tl_derived_price_change_5m: Option<f64>,
    pub tl_derived_price_change_10m: Option<f64>,
    pub tl_derived_price_vol_1m: Option<f64>,
    pub tl_derived_price_vol_5m: Option<f64>,
    pub tl_derived_price_vol_10m: Option<f64>,
    pub next_funding_rate: f64,
    pub next_funding_at: &'a str,
    pub min_order_size: f64,
    pub instrument_type: &'a str,
    pub initial_margin_fraction: f64,
    pub maintenance_margin_fraction: f64,
    pub baseline_position_size: f64,
    pub incremental_position_size: f64,
    pub incremental_initial_margin_fraction: f64,
    pub volume_24h: f64,
    pub trades_24h: f64,
    pub open_interest: f64,
    pub max_position_size: f64,
    pub asset_resolution: f64,
}


impl fmt::Display for TLDYDXMarket<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<10} {:<10} {:>10} {:>10} 
            {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} 
            {:>10.4} 
            {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} 
            {:>10.4} {:>10.4} {:>10.4}  
            {:>10.4}
            {:>10} 
            {:>10.4} 
            {:>10} 
            {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4}", 
            self.snapshot_date, self.market, self.status, self.base_asset, self.quote_asset, 
            self.step_size, self.tick_size, self.index_price, self.oracle_price, self.tl_derived_index_oracle_spread, 
            self.price_change_24h, 
            self.tl_derived_price_change_5s.unwrap_or(0.),self.tl_derived_price_change_10s.unwrap_or(0.),self.tl_derived_price_change_30s.unwrap_or(0.),self.tl_derived_price_change_1m.unwrap_or(0.),self.tl_derived_price_change_5m.unwrap_or(0.),self.tl_derived_price_change_10m.unwrap_or(0.),
            self.tl_derived_price_vol_1m.unwrap_or(0.),self.tl_derived_price_vol_5m.unwrap_or(0.),self.tl_derived_price_vol_10m.unwrap_or(0.),            
            self.next_funding_rate, 
            self.next_funding_at, 
            self.min_order_size, 
            self.instrument_type, 
            self.initial_margin_fraction, self.maintenance_margin_fraction, self.baseline_position_size, self.incremental_position_size, self.incremental_initial_margin_fraction, self.volume_24h, self.trades_24h, self.open_interest, self.max_position_size, self.asset_resolution)
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
    pub oracle_price: String,
    #[serde(rename(deserialize = "priceChange24H"))]
    pub price_change_24h: String,
    #[serde(rename(deserialize = "nextFundingRate"))]
    pub next_funding_rate: String,
    #[serde(rename(deserialize = "nextFundingAt"))]
    pub next_funding_at: String,
    #[serde(rename(deserialize = "minOrderSize"))]
    pub min_order_size: String,
    #[serde(rename(deserialize = "type"))]
    pub instrument_type: String,
    #[serde(rename(deserialize = "initialMarginFraction"))]
    pub initial_margin_fraction: String,
    #[serde(rename(deserialize = "maintenanceMarginFraction"))]
    pub maintenance_margin_fraction: String,
    #[serde(rename(deserialize = "baselinePositionSize"))]
    pub baseline_position_size: String,
    #[serde(rename(deserialize = "incrementalPositionSize"))]
    pub incremental_position_size: String,
    #[serde(rename(deserialize = "incrementalInitialMarginFraction"))]
    pub incremental_initial_margin_fraction: String,
    #[serde(rename(deserialize = "volume24H"))]
    pub volume_24h: String,
    #[serde(rename(deserialize = "trades24H"))]
    pub trades_24h: String,
    #[serde(rename(deserialize = "openInterest"))]
    pub open_interest: String,
    #[serde(rename(deserialize = "maxPositionSize"))]
    pub max_position_size: String,
    #[serde(rename(deserialize = "assetResolution"))]
    pub asset_resolution: String,

}


impl fmt::Display for DYDXMarket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}", self.market, self.status, self.base_asset, self.quote_asset, self.step_size, self.tick_size, self.index_price, self.oracle_price, self.price_change_24h, self.next_funding_rate, self.next_funding_at, self.min_order_size, self.instrument_type, self.initial_margin_fraction, self.maintenance_margin_fraction, self.baseline_position_size, self.incremental_position_size, self.incremental_initial_margin_fraction, self.volume_24h, self.trades_24h, self.open_interest, self.max_position_size, self.asset_resolution)
    }
}


#[derive(Deserialize, Debug)]
pub struct DYDXMarkets {
    #[serde(rename(deserialize = "markets"))]
    pub markets: HashMap<String,DYDXMarket>
}
