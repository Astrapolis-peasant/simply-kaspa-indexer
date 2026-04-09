use kaspa_hashes::Hash;
use sqlx::{PgPool, Postgres, Transaction};

use crate::models::IndexBatch;
use crate::processor::hash_to_bytes;

pub async fn delete_reorged_acceptances(pool: &PgPool, removed_hashes: &[Hash]) {
    let mut db_txn = pool.begin().await.expect("begin reorg transaction");
    for &removed_hash in removed_hashes {
        sqlx::query("DELETE FROM transactions_acceptances WHERE block_hash = $1")
            .bind(hash_to_bytes(removed_hash))
            .execute(db_txn.as_mut())
            .await
            .expect("delete reorged acceptances");
    }
    db_txn.commit().await.expect("commit reorg transaction");
}

pub async fn load_checkpoint(pool: &PgPool) -> Option<Hash> {
    let row: Option<(String,)> = sqlx::query_as("SELECT value FROM vars WHERE key = 'vcp_start_hash'")
        .fetch_optional(pool)
        .await
        .expect("load checkpoint");
    row.map(|(v,)| v.parse::<Hash>().expect("invalid checkpoint hash"))
}

pub async fn save_checkpoint(db_txn: &mut Transaction<'_, Postgres>, hash: Hash) {
    sqlx::query(
        "INSERT INTO vars (key, value) VALUES ('vcp_start_hash', $1)
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(hash.to_string())
    .execute(db_txn.as_mut())
    .await
    .expect("save checkpoint");
}

fn placeholders(rows: usize, cols: usize) -> String {
    (0..rows)
        .map(|r| {
            let params = (0..cols)
                .map(|c| format!("${}", r * cols + c + 1))
                .collect::<Vec<_>>()
                .join(", ");
            format!("({params})")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub async fn commit_index_batch(pool: &PgPool, batch: &IndexBatch, checkpoint: Hash) -> (usize, usize) {
    let mut db_txn = pool.begin().await.expect("begin transaction");

    // 1. blocks
    if !batch.blocks.is_empty() {
        let sql = format!(
            "INSERT INTO blocks
             (hash, accepted_id_merkle_root, bits, blue_score, blue_work,
              daa_score, hash_merkle_root, nonce, pruning_point,
              timestamp, utxo_commitment, version)
             VALUES {} ON CONFLICT (hash) DO NOTHING",
            placeholders(batch.blocks.len(), 12)
        );
        let mut query = sqlx::query(&sql);
        for b in &batch.blocks {
            query = query
                .bind(&b.hash)
                .bind(&b.accepted_id_merkle_root)
                .bind(b.bits)
                .bind(b.blue_score)
                .bind(&b.blue_work)
                .bind(b.daa_score)
                .bind(&b.hash_merkle_root)
                .bind(&b.nonce)
                .bind(&b.pruning_point)
                .bind(b.timestamp)
                .bind(&b.utxo_commitment)
                .bind(b.version);
        }
        query.execute(db_txn.as_mut()).await.expect("insert blocks");
    }

    // 2. block_parent
    if !batch.block_parents.is_empty() {
        let sql = format!(
            "INSERT INTO block_parent (block_hash, parent_hash) VALUES {} ON CONFLICT DO NOTHING",
            placeholders(batch.block_parents.len(), 2)
        );
        let mut query = sqlx::query(&sql);
        for bp in &batch.block_parents {
            query = query.bind(&bp.block_hash).bind(&bp.parent_hash);
        }
        query.execute(db_txn.as_mut()).await.expect("insert block_parent");
    }

    // 3. blocks_transactions
    if !batch.block_transactions.is_empty() {
        let sql = format!(
            "INSERT INTO blocks_transactions (block_hash, transaction_id) VALUES {} ON CONFLICT DO NOTHING",
            placeholders(batch.block_transactions.len(), 2)
        );
        let mut query = sqlx::query(&sql);
        for bt in &batch.block_transactions {
            query = query.bind(&bt.block_hash).bind(&bt.transaction_id);
        }
        query.execute(db_txn.as_mut()).await.expect("insert blocks_transactions");
    }

    // 4. transactions_acceptances
    if !batch.transaction_acceptances.is_empty() {
        let sql = format!(
            "INSERT INTO transactions_acceptances (transaction_id, block_hash)
             VALUES {}
             ON CONFLICT (transaction_id) DO UPDATE SET block_hash = EXCLUDED.block_hash",
            placeholders(batch.transaction_acceptances.len(), 2)
        );
        let mut query = sqlx::query(&sql);
        for ta in &batch.transaction_acceptances {
            query = query.bind(&ta.transaction_id).bind(&ta.block_hash);
        }
        query.execute(db_txn.as_mut()).await.expect("insert transactions_acceptances");
    }

    // 5. transactions
    if !batch.transactions.is_empty() {
        let sql = format!(
            "INSERT INTO transactions
             (transaction_id, subnetwork_id, hash, mass, payload, block_time, version, inputs, outputs)
             VALUES {} ON CONFLICT (transaction_id) DO NOTHING",
            placeholders(batch.transactions.len(), 9)
        );
        let mut query = sqlx::query(&sql);
        for t in &batch.transactions {
            query = query
                .bind(&t.transaction_id)
                .bind(&t.subnetwork_id)
                .bind(&t.hash)
                .bind(t.mass)
                .bind(&t.payload)
                .bind(t.block_time)
                .bind(t.version)
                .bind(&t.inputs)
                .bind(&t.outputs);
        }
        query.execute(db_txn.as_mut()).await.expect("insert transactions");
    }

    // 6. addresses_transactions
    if !batch.address_transactions.is_empty() {
        let sql = format!(
            "INSERT INTO addresses_transactions (address, transaction_id, block_time)
             VALUES {} ON CONFLICT DO NOTHING",
            placeholders(batch.address_transactions.len(), 3)
        );
        let mut query = sqlx::query(&sql);
        for at in &batch.address_transactions {
            query = query.bind(&at.address).bind(&at.transaction_id).bind(at.block_time);
        }
        query.execute(db_txn.as_mut()).await.expect("insert addresses_transactions");
    }

    save_checkpoint(&mut db_txn, checkpoint).await;
    db_txn.commit().await.expect("commit transaction");

    (batch.transaction_acceptances.len(), batch.address_transactions.len())
}
