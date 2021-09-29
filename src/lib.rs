pub mod models;

use self::models::{TimeRange};

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



pub const LOCAL_MONGO: &str = "mongodb://localhost:27017";
pub const THE_DATABASE: &str = "tradellama";
pub const THE_CRYPTO_COLLECTION: &str = "crypto";
pub const THE_CRYPTO_RBMS_COLLECTION: &str = "rbms";
pub const THE_CRYPTO_RBES_COLLECTION: &str = "rbes";
pub const THE_CRYPTO_LIQUIDATION_COLLECTION: &str = "cryptoliquidation";
pub const THE_CRYPTOCLUSTER_COLLECTION: &str = "cryptocluster";
pub const THE_CRYPTO_CAP_SUITE_COLLECTION: &str = "cryptocapsuite";



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




pub fn formatted_company<'a>(company: &str) -> &str {
    match company {
        "rivernorth" => "RiverNorth Capital Management LLC",
        _ => "ERROR"
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
        "rivernorth" => match tx {
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
        "rivernorth" => match tx {
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
        "rivernorth" => match tx {
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
        "rivernorth" => match tx {
            "COVER" | "BUYC" | "COVERSHORT" | "BUYCOVER" | "Cover" => true,
            _ => false,
        },
        _ => match tx {
            "COVER" | "BUYC" | "COVERSHORT" | "Cover" => true,
            _ => false,
        },
    }
}


pub fn mask_account<'a>(company: &'a str, aid: &'a str) -> &'a str {
    match company {
        "rivernorth" => match aid {
            "DMSFX" => "Carter Endowment",
            "FGP" => "Reagan Pension",
            "OMOIX" => "Kennedy Trust",
            "OPP" => "Washington Roth IRA",
            "RFM" => "Truman Inc",
            "RIV" => "Wilson Inc",
            "RMI" => "Bush LLC Pension",
            "RMM" => "Obama T2",
            "RNCOX" => "Bush LLC 401k",
            "RNCP" => "Trump Roth IRA",
            "RNGIX" => "Lincoln Pension",
            "RNHIX" => "Grant IRAs",
            "RNIP" => "Biden Offshore Trust",
            "RNSAF" => "Buchanan Trust",
            "RNSIX" => "Wilson 401K",
            "RSF" => "Obama Children Trust",
            "RV1J" => "Bush Oil 401K",
            "SS DMSFX" => "Clinton Library Pension",
            "UMF" => "Clinton Family Trust",
            "VFLEX" => "Obama T1",
            "VAM" => "Johnson Trust",
            "VIV_BNP" => "Johnson Offshore LLC",
            "XVAMX" => "Biden China Inc",
            "752008483" => "Jackson Endowment",
            "752023399" => "Hamilton IRA",
            _ => aid,
        },
        _ => match aid {
            _ => aid,
        },
    }
}




pub fn get_time_range<'a>(
    d1: &'a str,
    d2: &'a str,
    df: &'a str,
) -> Result<TimeRange, Box<dyn Error>> {
    let d1 = NaiveDateTime::parse_from_str(d1, df)?;
    let d2 = NaiveDateTime::parse_from_str(d2, df)?;
    let utcd1 = DateTime::<Utc>::from_utc(d1, Utc);
    let utcd2 = DateTime::<Utc>::from_utc(d2, Utc);

    let time_range = TimeRange {
        gtedate: utcd1,
        ltdate: utcd2,
    };

    Ok(time_range)
}

pub fn  get_time_ranges<'a>(
    d1: &'a str,
    d2: &'a str,
    df: &'a str,
    dint: &'a i64,
) -> Result<Vec<TimeRange>, Box<dyn Error>> {
    let d1 = NaiveDateTime::parse_from_str(d1, df)?;
    let d2 = NaiveDateTime::parse_from_str(d2, df)?;
    // let utcd1 = DateTime::<Utc>::from_utc(d1,Utc);
    // let utcd2 = DateTime::<Utc>::from_utc(d2,Utc);

    let mut dt = d1;

    let mut time_ranges = Vec::new();

    while dt < d2 {
        let gtd = dt;
        let ltd = gtd + Duration::days(*dint);
        dt = dt + Duration::days(1);
        let tr = TimeRange {
            gtedate: DateTime::<Utc>::from_utc(gtd, Utc),
            ltdate: DateTime::<Utc>::from_utc(ltd, Utc),
        };
        time_ranges.push(tr);
    }

    Ok(time_ranges)
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

    let mut rvec = Vec::new();

    for idx in 0..(v.len() / 2) {
        let k = targets[idx] as i32;
        rvec.push(k);
    }

    assert_eq!(v.len() / 2, rvec.len());

    rvec

}





#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
