use futures::stream::TryStreamExt;
use mongodb::{bson::doc, options::ClientOptions, Client, Collection, Cursor};
use serde::{Deserialize, Serialize};
use duckdb::{params, Connection, Result};

#[derive(Debug, Serialize, Deserialize)]
struct Contact {
    name: String,
    email: String,
    #[serde(rename = "jgiSsoId")]
    jgi_sso_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Study {
    #[serde(rename = "studyGoldId")]
    study_gold_id: String,
    contacts: Vec<Contact>,
    #[serde(flatten)]
    extra: std::collections::BTreeMap<String, serde_json::Value>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection_string = "mongodb://localhost:27017";

    let client_options = ClientOptions::parse(connection_string).await?;
    let client = Client::with_options(client_options)?;

    let db = client.database("gold_metadata");
    let collection: Collection<Study> = db.collection("studies");

    let find_options = mongodb::options::FindOptions::builder().limit(3).build();
    let mut cursor: Cursor<Study> = collection.find(doc! {}, find_options).await?;

    let duckdb_conn = Connection::open("my_studies.db")?;
    
    duckdb_conn.execute(
        "CREATE TABLE IF NOT EXISTS contacts (
            study_gold_id VARCHAR,
            contact_name VARCHAR,
            contact_email VARCHAR,
            contact_jgi_sso_id VARCHAR
        )",
        [],
    )?;
    
    while let Some(study) = cursor.try_next().await? {
        let mut stmt = duckdb_conn.prepare("INSERT INTO contacts (study_gold_id, contact_name, contact_email, contact_jgi_sso_id) VALUES (?, ?, ?, ?)")?;
    
        for contact in study.contacts {
            stmt.execute(params![
                study.study_gold_id.as_str(),
                contact.name.as_str(),
                contact.email.as_str(),
                contact.jgi_sso_id.as_deref().unwrap_or(""),
            ])?;
        }
    }
    
    Ok(())
}