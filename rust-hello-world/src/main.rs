use clap::Parser;
use duckdb::{params, Connection, Result};
use futures::stream::TryStreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use mongodb::{bson::doc, options::ClientOptions, Client, Collection, Cursor};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Process biosample attributes from MongoDB to DuckDB
#[derive(Parser, Debug)]
struct Args {
    /// MongoDB connection string
    #[arg(long, default_value = "mongodb://localhost:27017")]
    mongo_uri: String,

    /// MongoDB database name
    #[arg(long, default_value = "biosamples")]
    mongo_db: String,

    /// MongoDB collection name
    #[arg(long, default_value = "biosamples")]
    mongo_collection: String,

    /// Output DuckDB database file
    #[arg(long, default_value = "biosample_attributes.db")]
    output_db: String,

    /// Number of biosamples to process (0 for all)
    #[arg(long, default_value = "10")]
    limit: i64,

    /// Total number of biosamples in MongoDB
    #[arg(long, default_value = "45000000")]
    total_biosamples: u64,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
}

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.verbose {
        println!("Connecting to MongoDB at {}", args.mongo_uri);
    }

    let client_options = ClientOptions::parse(&args.mongo_uri).await?;
    let client = Client::with_options(client_options)?;

    let db = client.database(&args.mongo_db);
    let collection: Collection<Biosample> = db.collection(&args.mongo_collection);

    let find_options = if args.limit > 0 {
        mongodb::options::FindOptions::builder()
            .limit(args.limit)
            .build()
    } else {
        mongodb::options::FindOptions::builder().build()
    };

    if args.verbose {
        println!("Setting up DuckDB at {}", args.output_db);
    }

    let mut cursor: Cursor<Biosample> = collection.find(doc! {}, find_options).await?;

    let duckdb_conn = Connection::open(&args.output_db)?;

    // Drop existing tables if they exist
    duckdb_conn.execute("DROP TABLE IF EXISTS attribute", [])?;
    duckdb_conn.execute("DROP TABLE IF EXISTS package", [])?;

    // Create tables with fixed schemas
    duckdb_conn.execute(
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

    duckdb_conn.execute(
        "CREATE TABLE package (
            content VARCHAR,
            display_name VARCHAR,
            id BIGINT
        )",
        [],
    )?;

    // Setup progress bar
    let total = if args.limit > 0 {
        args.limit as u64
    } else {
        args.total_biosamples
    };
    let pb = ProgressBar::new(total);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({per_sec}, {eta})")
        .unwrap()
        .progress_chars("#>-"));

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

        // Process Attributes
        if let Some(attributes) = biosample.attributes {
            if let Some(attribute_obj) = attributes.as_object() {
                if let Some(attribute_array) = attribute_obj.get("Attribute") {
                    if let Some(attrs) = attribute_array.as_array() {
                        if args.verbose {
                            println!("  Found {} attributes", attrs.len());
                        }
                        for attr_value in attrs {
                            if let Ok(attr) =
                                serde_json::from_value::<AttributeData>(attr_value.clone())
                            {
                                if !attr.extra.is_empty() && args.verbose {
                                    println!("  Extra attribute fields found: {:?}", attr.extra);
                                }

                                attr_stmt.execute(params![
                                    attr.content.as_deref().unwrap_or(""),
                                    attr.attribute_name.as_deref().unwrap_or(""),
                                    biosample.id.parse::<i64>().unwrap_or(-1),
                                    attr.harmonized_name.as_deref().unwrap_or(""),
                                    attr.display_name.as_deref().unwrap_or(""),
                                    attr.unit.as_deref().unwrap_or(""),
                                ])?;
                            } else if args.verbose {
                                println!("  Failed to parse attribute: {:?}", attr_value);
                            }
                        }
                    } else if args.verbose {
                        println!("  Attribute is not an array");
                    }
                } else if args.verbose {
                    println!("  No Attribute field found");
                }
            } else if args.verbose {
                println!("  Attributes is not an object");
            }
        } else if args.verbose {
            println!("  No Attributes found");
        }

        // Process Package
        if let Some(package) = &biosample.package {
            match serde_json::from_value::<PackageData>(package.clone()) {
                Ok(pkg) => {
                    if !pkg.extra.is_empty() && args.verbose {
                        println!(
                            "Extra package fields found for biosample {}: {:?}",
                            biosample.id, pkg.extra
                        );
                    }

                    pkg_stmt.execute(params![
                        pkg.content.as_deref().unwrap_or(""),
                        pkg.display_name.as_deref().unwrap_or(""),
                        biosample.id.parse::<i64>().unwrap_or(-1),
                    ])?;
                }
                Err(e) => {
                    if args.verbose {
                        println!(
                            "Failed to parse package for biosample {}: {:?}",
                            biosample.id, e
                        );
                    }
                }
            }
        }
    }

    pb.finish_with_message("Processing complete");
    println!("Processed {} biosamples total", processed_count);

    Ok(())
}
