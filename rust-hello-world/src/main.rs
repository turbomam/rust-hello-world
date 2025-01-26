use clap::Parser;
use duckdb::{params, Connection, Result};
use futures::stream::TryStreamExt;
use mongodb::{bson::doc, options::ClientOptions, Client, Collection, Cursor};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Process biosample attributes from MongoDB to DuckDB
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
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
struct Biosample {
    id: String,
    #[serde(rename = "Attributes")]
    attributes: Option<serde_json::Value>,
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

    // Drop existing table if it exists
    duckdb_conn.execute("DROP TABLE IF EXISTS attribute", [])?;

    // Create table with exact schema from DBeaver
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

    let mut biosample_count = 0;
    let mut attr_id = 1i64; // Counter for the id column

    while let Some(biosample) = cursor.try_next().await? {
        biosample_count += 1;
        if args.verbose {
            println!(
                "Processing biosample {} (ID: {})",
                biosample_count, biosample.id
            );
        }

        let mut stmt = duckdb_conn.prepare(
            "INSERT INTO attribute (content, attribute_name, id, harmonized_name, display_name, unit)
             VALUES (?, ?, ?, ?, ?, ?)"
        )?;

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
                                    println!("  Extra fields found: {:?}", attr.extra);
                                }

                                stmt.execute(params![
                                    attr.content.as_deref().unwrap_or(""),
                                    attr.attribute_name.as_deref().unwrap_or(""),
                                    attr_id,
                                    attr.harmonized_name.as_deref().unwrap_or(""),
                                    attr.display_name.as_deref().unwrap_or(""),
                                    attr.unit.as_deref().unwrap_or(""),
                                ])?;

                                attr_id += 1;
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
    }

    println!("Processed {} biosamples total", biosample_count);

    Ok(())
}
