use color_eyre::Result as EyreResult;
use eyre::WrapErr;
use serde::Deserialize;
use dotenv::dotenv;
// use tracing::{info,instrument};
// use tracing_subscriber::EnvFilter;

#[derive(Debug,Deserialize)]
pub struct Config {
	pub local_mongo: String,
	pub tldb: String,
	pub local_kafka_broker: String,
	pub api_secret: String,
	pub api_token: String
}

impl Config {
//	#[instrument]
	pub fn from_env() -> EyreResult<Config> {
		dotenv().ok();
//		tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env()).init();
//		info!("Loading configuration");
		let mut c = config::Config::new();
		c.merge(config::Environment::default())?;
		c.try_into().context("Loading configuration from environment.")
	}
	

}