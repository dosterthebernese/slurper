pub mod models;
//pub mod dydx_models;


use std::collections::HashMap;

use log::{debug,error};
use std::error::Error;
use std::fmt;
use charts::{Chart, VerticalBarView, ScaleBand, ScaleLinear, BarLabelPosition, AxisPosition, Color};
use chrono::{NaiveDateTime, DateTime, Utc};
use time::Duration;

use linfa::traits::Predict;
use linfa::DatasetBase;
use linfa_clustering::{KMeans};

use ndarray::Array;
use ndarray::{Axis, array};
use ndarray_rand::rand::SeedableRng;
use rand_isaac::Isaac64Rng;



//pub const LOCAL_MONGO: &str = "mongodb://localhost:27017";
//pub const THE_DATABASE: &str = "tradellama";
pub const THE_SOURCE_THING_LAST_UPDATE_COLLECTION: &str = "sourcethinglastupdate";
pub const THE_CRYPTO_TRADES_COLLECTION: &str = "crypto_trades";
pub const THE_TRADELLAMA_DYDX_SNAPSHOT_COLLECTION: &str = "tldydxsnap";
pub const THE_TRADELLAMA_DYDX_ORDERBOOK_COLLECTION: &str = "tldydxorderbook";


// kinda dead jim
pub const THE_CRYPTO_RBMS_COLLECTION: &str = "rbms";
pub const THE_CRYPTO_RBES_COLLECTION: &str = "rbes";
pub const THE_CRYPTO_LIQUIDATION_COLLECTION: &str = "cryptoliquidation";
pub const THE_CRYPTOCLUSTER_COLLECTION: &str = "cryptocluster";
pub const THE_CRYPTO_CAP_SUITE_COLLECTION: &str = "cryptocapsuite";
pub const THE_TRADELLAMA_PHEMEX_MD_SNAPSHOT_COLLECTION: &str = "tlphsnap";



#[derive(Debug)]
struct StrError<'a>(&'a str);

// Error doesn't require you to implement any methods, but
// your type must also implement Debug and Display.
impl<'a> Error for StrError<'a> {}

impl<'a> fmt::Display for StrError<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Delegate to the Display impl for `&str`:
        self.0.fmt(f)
    }
}





pub fn stacked_column<'a>(company: &str, chart_title: &str, bottom_label: &str, fname: &str, scalex: f64, scaley: f64, labels: Vec<String>, data: Vec<(String,f32,String)>) -> Result<(), Box<dyn Error>> {

    debug!("Processing {:?} {:?} {:?} {:?}", company, fname, labels, data);

    let width = 1200;
    let height = 800;
    let (top, right, bottom, left) = (90, 40, 150, 60);

    let x = ScaleBand::new()
        .set_domain(labels)
        .set_range(vec![0, width - left - right]);

    let y = ScaleLinear::new()
        .set_domain(vec![scalex as f32, scaley as f32])
        .set_range(vec![height - top - bottom, 0]);

    //MSFT
    let vhx = vec!["#7FBA00","#F25022","#00A4EF","#FFB900","#737373"];

    // Create VerticalBar view that is going to represent the data as vertical bars.
    let view = VerticalBarView::new()
        .set_x_scale(&x)
        .set_y_scale(&y)
        .set_label_position(BarLabelPosition::Center) // NOT USE IF VISI FALSE
        .set_label_rounding_precision(2) // NOT USE IF VISI FALSE
        .set_label_visibility(false)  // <-- uncomment this line to hide bar value labels
        .set_colors(Color::from_vec_of_hex_strings(vhx))
        //.set_colors(Color::color_scheme_tableau_10())
        .load_data(&data).unwrap();

    // Generate and save the chart.
    Chart::new()
        .set_width(width)
        .set_height(height)
        .set_margins(top, right, bottom, left)
        .add_title(String::from(chart_title))
        .add_view(&view)
        .add_axis_bottom(&x)
        .add_axis_left(&y)
        .add_left_axis_label("USD in Millions")
        .add_legend_at(AxisPosition::Bottom)
        .add_bottom_axis_label(bottom_label)
        .set_bottom_axis_tick_label_rotation(0)
        .save(fname).unwrap();

    Ok(())
}



pub fn hack_month_year_vector_from_2018 <'a>() -> Vec<String> {
    vec!["2018 01".to_string(),"2018 02".to_string(),"2018 03".to_string(),"2018 04".to_string(),"2018 05".to_string(),"2018 06".to_string(),"2018 07".to_string(),"2018 08".to_string(),"2018 09".to_string(),"2018 10".to_string(),"2018 11".to_string(),"2018 12".to_string(),
    "2019 01".to_string(),"2019 02".to_string(),"2019 03".to_string(),"2019 04".to_string(),"2019 05".to_string(),"2019 06".to_string(),"2019 07".to_string(),"2019 08".to_string(),"2019 09".to_string(),"2019 10".to_string(),"2019 11".to_string(),"2019 12".to_string(),
    "2020 01".to_string(),"2020 02".to_string(),"2020 03".to_string(),"2020 04".to_string(),"2020 05".to_string(),"2020 06".to_string(),"2020 07".to_string(),"2020 08".to_string(),"2020 09".to_string(),"2020 10".to_string(),"2020 11".to_string(),"2020 12".to_string(),
    "2021 01".to_string(),"2021 02".to_string(),"2021 03".to_string(),"2021 04".to_string(),"2021 05".to_string(),"2021 06".to_string(),"2021 07".to_string(),"2021 08".to_string(),"2021 09".to_string(),"2021 10".to_string(),"2021 11".to_string(),"2021 12".to_string()]
}


pub fn parse_config_three(args: &[String]) -> (&str, &str, &str) {
    let q = &args[0];
    let r = &args[1];
    let l = &args[2];
    (q,r,l)
}

pub fn parse_config_two(args: &[String]) -> (&str, &str) {
    let q = &args[0];
    let r = &args[1];
    (q,r)
}


pub fn contra<'a>(company: &'a str, tx: &'a str) -> &'a str {
    if am_buy(company, tx) {
        "Sell"
    } else if am_sell(company,tx) {
        "Buy"
    } else if am_short(company,tx) {
        "Cover"
    } else if am_cover(company,tx) {
        "Short"
    } else {
        error!("This is very bad - this tx should never have been passed, error upstream: {:?}", tx);
        "WTF - VERY BAD ERROR"
    }
}

pub fn am_buy<'a>(company: &'a str, tx: &'a str) -> bool {
    match company {
        "bigbadvoodoodaddy" => match tx {
            "BUY" | "BUYL" | "Buy" | "REINVEST" => true,
            _ => false,
        },
        _ => match tx {
            "BUY" | "BUYL" | "Buy" => true,
            _ => false,
        },
    }
}

pub fn am_sell<'a>(company: &'a str, tx: &'a str) -> bool {
    match company {
        "bigbadvoodoodaddy" => match tx {
            "SELL" | "SELLL" | "Sell" => true,
            _ => false,
        },
        _ => match tx {
            "SELL" | "SELLL" | "Sell" => true,
            _ => false,
        },
    }
}


pub fn am_short<'a>(company: &'a str, tx: &'a str) -> bool {
    match company {
        "bigbadvoodoodaddy" => match tx {
            "SHORT" | "SELLS" | "SELLSHORT" | "Short" => true,
            _ => false,
        },
        _ => match tx {
            "SHORT" | "SELLS" | "SELLSHORT" | "Short" => true,
            _ => false,
        },
    }
}

pub fn am_cover<'a>(company: &'a str, tx: &'a str) -> bool {
    match company {
        "bigbadvoodoodaddy" => match tx {
            "COVER" | "BUYC" | "COVERSHORT" | "BUYCOVER" | "Cover" => true,
            _ => false,
        },
        _ => match tx {
            "COVER" | "BUYC" | "COVERSHORT" | "Cover" => true,
            _ => false,
        },
    }
}






pub fn get_hour_in_day_index<'a>(trade_date: DateTime<Utc>) -> i64 {
    match trade_date.format("%H").to_string().as_str() {
        "00" => 0,
        "01" => 1,
        "02" => 2,
        "03" => 3,
        "04" => 4,
        "05" => 5,
        "06" => 6,
        "07" => 7,
        "08" => 8,
        "09" => 9,
        "10" => 10,
        "11" => 11,
        "12" => 12,
        "13" => 13,
        "14" => 14,
        "15" => 15,
        "16" => 16,
        "17" => 17,
        "18" => 18,
        "19" => 19,
        "20" => 20,
        "21" => 21,
        "22" => 22,
        "23" => 23,
        "24" => 24,
        _ => panic!("invalid match {:?}", trade_date.format("%H").to_string().as_str()),
    }
}


pub fn get_day_of_week_index<'a>(trade_date: DateTime<Utc>) -> i64 {
    match trade_date.format("%A").to_string().as_str() {
        "Sunday" => 0,
        "Monday" => 1,
        "Tuesday" => 2,
        "Wednesday" => 3,
        "Thursday" => 4,
        "Friday" => 5,
        "Saturday" => 6,
        _ => panic!("invalid match"),
    }
}

pub fn get_day_of_month_index<'a>(trade_date: DateTime<Utc>) -> i64 {
    trade_date.format("%d").to_string().as_str().parse::<i64>().unwrap()
}


pub fn get_n_days_mf<'a>(trade_date: DateTime<Utc>, spread: &'a i32) -> DateTime<Utc> {
    let vdays = vec!["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"];
    let mut dt = trade_date;
    let mut idx = 0;
    while idx < *spread {
        let dow = dt.format("%A").to_string();
        let trackable = vdays.iter().any(|v| v == &dow);
        if trackable {
            idx += 1;
        }
        dt = dt + Duration::days(1);
    }
    let ltdate: DateTime<Utc> = dt;
    ltdate
}

pub fn get_mf_n_days<'a>(trade_date: DateTime<Utc>, quote_date: DateTime<Utc>) -> i32 {
    let vdays = vec!["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"];
    let mut idx = 0;
    if trade_date == quote_date {
        let dow = trade_date.format("%A").to_string();
        let trackable = vdays.iter().any(|v| v == &dow);
        // assumes you would only call this with a valid mf date
        assert_eq!(true, trackable);
        idx
    } else {
        let mut dt1 = trade_date;
        let dt2 = quote_date;
        while dt1 <= dt2 {
            let dow = dt1.format("%A").to_string();
            // debug!("dow: {:?}", &dow);
            let trackable = vdays.iter().any(|v| v == &dow);
            if trackable {
                idx += 1;
            }
            dt1 = dt1 + Duration::days(1);
        }
        // not really sure my brain too small but need the -1
        idx - 1
    }
}



pub fn mean(data: &[f64]) -> Option<f64> {
    let sum = data.iter().sum::<f64>() as f64;
    let count = data.len();

    match count {
        positive if positive > 0 => Some(sum / count as f64),
        _ => None,
    }
}

pub fn std_deviation(data: &[f64]) -> Option<f64> {
    match (mean(data), data.len()) {
        (Some(data_mean), count) if count > 0 => {
            let variance = data.iter().map(|value| {
                let diff = data_mean - (*value as f64);

                diff * diff
            }).sum::<f64>() / count as f64;

            Some(variance.sqrt())
        },
        _ => None
    }
}

pub fn get_interval_performance<'a>(current_price: f64, lookback: usize, market: &'a str, hm: &HashMap<String,Vec<f64>>) -> Option<f64> {

    if hm[market].len() >= (lookback)  {
        let retro_price_loc = hm[market].len() - lookback;
        match hm.get(market) {
            Some(old_prices) => {
                let retro_price = old_prices[retro_price_loc];
                let delta = (current_price - retro_price) / retro_price;
                debug!("retro spot is: {} giving us base of {} to measure against current {} for a delta of {}", retro_price_loc, retro_price, current_price, delta);
                Some(delta)
            },
            _ => None
        }

    } else {
        None
    }

}



pub fn get_interval_volatility<'a>(lookback: usize, market: &'a str, hm: &HashMap<String,Vec<f64>>) -> Option<f64> {

    if hm[market].len() >= (lookback)  {
        let retro_price_loc = hm[market].len() - lookback;
        match hm.get(market) {
            Some(old_prices) => {
                let hcol: Vec<_> = (retro_price_loc..hm[market].len()).map(|n| old_prices[n]).collect();
                debug!("{:?} {:?} {:?} {:?}", hcol, old_prices, hcol.len(), old_prices.len());
                std_deviation(&hcol)
            },
            _ => None
        }

    } else {
        None
    }


}

// not weighted
pub fn get_interval_mean<'a>(lookback: usize, market: &'a str, hm: &HashMap<String,Vec<f64>>) -> Option<f64> {

    if hm[market].len() >= (lookback)  {
        let retro_price_loc = hm[market].len() - lookback;
        match hm.get(market) {
            Some(old_prices) => {
                let hcol: Vec<_> = (retro_price_loc..hm[market].len()).map(|n| old_prices[n]).collect();
                debug!("{:?} {:?} {:?} {:?}", hcol, old_prices, hcol.len(), old_prices.len());
                mean(&hcol)
            },
            _ => None
        }

    } else {
        None
    }


}



pub fn delta_walk_integers<'a>(v: &Vec<i64>) -> Vec<f64> {
    let mut rvec = Vec::new();
    for (idx, i) in v.iter().enumerate() {
        if idx == 0 {
            rvec.push(1.0);
        } else {
            let prior_value = v[idx-1];
            if prior_value != 0 {
                rvec.push( ((*i as f64 - prior_value as f64) / prior_value as f64) * 100. );            
            } else {
                rvec.push(*i as f64);
            }
        }
    }
    rvec
}

pub fn interval_walk_timestamps<'a>(v: &Vec<DateTime<Utc>>) -> Vec<i64> {
    let mut rvec = Vec::new();
    for (idx, i) in v.iter().enumerate() {
        if idx == 0 {
            rvec.push(0);
        } else {
            let prior_value = v[idx-1];
            rvec.push(i.signed_duration_since(prior_value).num_seconds());            
        }
    }
    rvec
}


pub fn do_duo_kmeans<'a>(v: &Vec<f64>) -> Vec<i32> {

    let rng = Isaac64Rng::seed_from_u64(42);
    let expected_centroids = array![[1000., 4.], [10000., 3.], [100000., 2.], [1000000., 1.],];
//    let n = 10000;
    let zdataset =  Array::from_shape_vec((v.len() / 2, 2), v.to_vec()).unwrap();
    let dataset = DatasetBase::from(zdataset);
    let n_clusters = expected_centroids.len_of(Axis(0));
    let model = KMeans::params_with_rng(n_clusters, rng)
        .max_n_iterations(200)
        .tolerance(1e-5)
        .fit(&dataset)
        .expect("KMeans fitted");
    let dataset = model.predict(dataset);
    let DatasetBase {
        records, targets, ..
    } = dataset;

    debug!("records for records warning {:?}", records);
    let mut rvec = Vec::new();

    for idx in 0..(v.len() / 2) {
        let k = targets[idx] as i32;
        rvec.push(k);
    }

    assert_eq!(v.len() / 2, rvec.len());

    rvec

}


pub fn do_triple_kmeans<'a>(v: &Vec<f64>) -> Vec<i32> {

    let rng = Isaac64Rng::seed_from_u64(42);
    let expected_centroids = array![[1000., 1000., 4.], [10000., 10000., 3.], [100000., 100000., 2.], [1000000., 1000000., 1.],];
//    let n = 10000;
    let zdataset =  Array::from_shape_vec((v.len() / 3, 3), v.to_vec()).unwrap();
    let dataset = DatasetBase::from(zdataset);
    let n_clusters = expected_centroids.len_of(Axis(0));
    let model = KMeans::params_with_rng(n_clusters, rng)
        .max_n_iterations(200)
        .tolerance(1e-5)
        .fit(&dataset)
        .expect("KMeans fitted");
    let dataset = model.predict(dataset);
    let DatasetBase {
        records, targets, ..
    } = dataset;

    debug!("records for records warning {:?}", records);
    let mut rvec = Vec::new();

    for idx in 0..(v.len() / 3) {
        let k = targets[idx] as i32;
        rvec.push(k);
    }

    assert_eq!(v.len() / 3, rvec.len());

    rvec

}




#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
