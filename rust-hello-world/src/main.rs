use futures::stream::TryStreamExt;  // Changed from the previous import
use mongodb::{bson::doc, options::ClientOptions, Client, Collection, Cursor};
use serde::{Deserialize, Serialize};
use duckdb::{params, Connection, Result};
use serde_json::Value;

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
    let connection_string = "mongodb://localhost:27017";

    let client_options = ClientOptions::parse(connection_string).await?;
    let client = Client::with_options(client_options)?;

    let db = client.database("biosamples");
    let collection: Collection<Biosample> = db.collection("biosamples");

    let find_options = mongodb::options::FindOptions::builder().limit(10).build();
    let mut cursor: Cursor<Biosample> = collection.find(doc! {}, find_options).await?;

    let duckdb_conn = Connection::open("biosample_attributes.db")?;

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
        println!(
            "Processing biosample {} (ID: {})",
            biosample_count, biosample.id
        );

        let mut stmt = duckdb_conn.prepare(
            "INSERT INTO attribute (content, attribute_name, id, harmonized_name, display_name, unit)
             VALUES (?, ?, ?, ?, ?, ?)"
        )?;

        if let Some(attributes) = biosample.attributes {
            if let Some(attribute_obj) = attributes.as_object() {
                if let Some(attribute_array) = attribute_obj.get("Attribute") {
                    if let Some(attrs) = attribute_array.as_array() {
                        println!("  Found {} attributes", attrs.len());
                        for attr_value in attrs {
                            if let Ok(attr) =
                                serde_json::from_value::<AttributeData>(attr_value.clone())
                            {
                                if !attr.extra.is_empty() {
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
                            } else {
                                println!("  Failed to parse attribute: {:?}", attr_value);
                            }
                        }
                    } else {
                        println!("  Attribute is not an array");
                    }
                } else {
                    println!("  No Attribute field found");
                }
            } else {
                println!("  Attributes is not an object");
            }
        } else {
            println!("  No Attributes found");
        }
    }

    println!("Processed {} biosamples total", biosample_count);

    Ok(())
}
