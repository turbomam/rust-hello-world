use clap::Parser;
use duckdb::{params, Connection, Result as DuckDBResult};
use futures::stream::TryStreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use mongodb::{bson::doc, options::ClientOptions, Client, Collection, Cursor};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;

#[derive(Debug, Serialize, Deserialize)]
struct AttributeData {
    content: Option<String>,
    attribute_name: Option<String>,
    harmonized_name: Option<String>,
    display_name: Option<String>,
    unit: Option<String>,
    #[serde(flatten)]
    extra: std::collections::BTreeMap<String, Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PackageData {
    content: Option<String>,
    display_name: Option<String>,
    #[serde(flatten)]
    extra: std::collections::BTreeMap<String, Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Biosample {
    id: String,
    #[serde(rename = "Attributes")]
    attributes: Option<serde_json::Value>,
    #[serde(rename = "Package")]
    package: Option<serde_json::Value>,
    #[serde(flatten)]
    extra: std::collections::BTreeMap<String, Value>,
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "mongodb://localhost:27017")]
    mongo_uri: String,

    #[arg(long, default_value = "biosamples")]
    mongo_db: String,

    #[arg(long, default_value = "biosamples")]
    mongo_collection: String,

    #[arg(long, default_value = "biosample_attributes.db")]
    output_db: String,

    #[arg(long, default_value = "10")]
    limit: i64,

    #[arg(long, default_value = "45000000")]
    total_biosamples: u64,

    #[arg(short, long)]
    verbose: bool,
}

async fn setup_mongodb(args: &Args) -> Result<Collection<Biosample>, Box<dyn Error>> {
    let client_options = ClientOptions::parse(&args.mongo_uri).await?;
    let client = Client::with_options(client_options)?;
    let db = client.database(&args.mongo_db);
    Ok(db.collection(&args.mongo_collection))
}

fn setup_duckdb(db_path: &str) -> DuckDBResult<Connection> {
    let conn = Connection::open(db_path)?;

    // Drop existing tables
    conn.execute("DROP TABLE IF EXISTS attribute", [])?;
    conn.execute("DROP TABLE IF EXISTS package", [])?;

    // Create tables with fixed schemas
    conn.execute(
        "CREATE TABLE attribute (
            content VARCHAR,
            attribute_name VARCHAR,
            id BIGINT,
            harmonized_name VARCHAR,
            display_name VARCHAR,
            unit VARCHAR
        )",
        [],
    )?;

    conn.execute(
        "CREATE TABLE package (
            content VARCHAR,
            display_name VARCHAR,
            id BIGINT
        )",
        [],
    )?;

    Ok(conn)
}

fn setup_progress_bar(total: u64) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({per_sec}, {eta})")
        .unwrap()
        .progress_chars("#>-"));
    pb
}

fn process_attributes(
    biosample: &Biosample,
    stmt: &mut duckdb::Statement,
    verbose: bool,
) -> DuckDBResult<()> {
    if let Some(attributes) = &biosample.attributes {
        if let Some(attribute_obj) = attributes.as_object() {
            if let Some(attribute_array) = attribute_obj.get("Attribute") {
                if let Some(attrs) = attribute_array.as_array() {
                    if verbose {
                        println!("  Found {} attributes", attrs.len());
                    }
                    for attr_value in attrs {
                        if let Ok(attr) =
                            serde_json::from_value::<AttributeData>(attr_value.clone())
                        {
                            if !attr.extra.is_empty() && verbose {
                                println!("  Extra attribute fields found: {:?}", attr.extra);
                            }

                            stmt.execute(params![
                                attr.content.as_deref().unwrap_or(""),
                                attr.attribute_name.as_deref().unwrap_or(""),
                                biosample.id.parse::<i64>().unwrap_or(-1),
                                attr.harmonized_name.as_deref().unwrap_or(""),
                                attr.display_name.as_deref().unwrap_or(""),
                                attr.unit.as_deref().unwrap_or(""),
                            ])?;
                        } else if verbose {
                            println!("  Failed to parse attribute: {:?}", attr_value);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn process_package(
    biosample: &Biosample,
    stmt: &mut duckdb::Statement,
    verbose: bool,
) -> DuckDBResult<()> {
    if let Some(package) = &biosample.package {
        match serde_json::from_value::<PackageData>(package.clone()) {
            Ok(pkg) => {
                if !pkg.extra.is_empty() && verbose {
                    println!(
                        "Extra package fields found for biosample {}: {:?}",
                        biosample.id, pkg.extra
                    );
                }

                stmt.execute(params![
                    pkg.content.as_deref().unwrap_or(""),
                    pkg.display_name.as_deref().unwrap_or(""),
                    biosample.id.parse::<i64>().unwrap_or(-1),
                ])?;
            }
            Err(e) => {
                if verbose {
                    println!(
                        "Failed to parse package for biosample {}: {:?}",
                        biosample.id, e
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    if args.verbose {
        println!("Connecting to MongoDB at {}", args.mongo_uri);
    }

    let collection = setup_mongodb(&args).await?;
    let duckdb_conn = setup_duckdb(&args.output_db)?;

    let find_options = if args.limit > 0 {
        mongodb::options::FindOptions::builder()
            .limit(args.limit)
            .build()
    } else {
        mongodb::options::FindOptions::builder().build()
    };

    let mut cursor: Cursor<Biosample> = collection.find(doc! {}, find_options).await?;

    let total = if args.limit > 0 {
        args.limit as u64
    } else {
        args.total_biosamples
    };
    let pb = setup_progress_bar(total);

    let mut processed_count = 0u64;

    let mut attr_stmt = duckdb_conn.prepare(
        "INSERT INTO attribute (content, attribute_name, id, harmonized_name, display_name, unit)
         VALUES (?, ?, ?, ?, ?, ?)",
    )?;

    let mut pkg_stmt = duckdb_conn.prepare(
        "INSERT INTO package (content, display_name, id)
         VALUES (?, ?, ?)",
    )?;

    while let Some(biosample) = cursor.try_next().await? {
        processed_count += 1;
        pb.set_position(processed_count);

        process_attributes(&biosample, &mut attr_stmt, args.verbose)?;
        process_package(&biosample, &mut pkg_stmt, args.verbose)?;
    }

    pb.finish_with_message("Processing complete");
    println!("Processed {} biosamples total", processed_count);

    Ok(())
}
