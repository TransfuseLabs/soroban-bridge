import { Tx } from "./core";

const sqlite3 = require("sqlite3").verbose();
const filepath = "./ionize-db.db";

export function createTable() {
  const db = new sqlite3.Database(filepath, (err: any) => {
    if (err) {
      console.error(err.message);
    }
  });

  // SQL statement to create a table if it does not exist
  const createTableQuery = `
        CREATE TABLE IF NOT EXISTS transactions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          from_network TEXT,
          from_address TEXT,
          from_asset_code TEXT,
          from_asset_issuer TEXT,
          from_amount TEXT,
          from_tx_hash TEXT,
          to_network TEXT,
          to_address TEXT,
          to_asset_code TEXT,
          to_asset_issuer TEXT,
          to_amount TEXT,
          to_tx_hash TEXT,
          tx_status TEXT,
          tx_fee REAL
        )
      `;

  // Execute the SQL query to create the table
  db.run(createTableQuery, (err: any) => {
    if (err) {
      console.error("Error creating table:", err.message);
    }
  });

  // Close the database connection
  db.close((err: any) => {
    if (err) {
      console.error(err.message);
    }
  });
}

// Function to insert a row into the SQLite database
export function insertTx(tx: Tx) {
  // Open the SQLite database
  const db = new sqlite3.Database(filepath, (err: any) => {
    if (err) {
      console.error(err.message);
    }
  });

  // SQL statement to insert a row into the table
  const insertQuery = `
  INSERT INTO transactions (
    from_network, from_address, from_asset_code, from_asset_issuer, from_amount, from_tx_hash,
    to_network, to_address, to_asset_code, to_asset_issuer, to_amount, to_tx_hash,
    tx_status, tx_fee
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`;

  // Bind parameters to the SQL statement
  const params = [
    tx.from_network,
    tx.from_address,
    tx.from_asset_code,
    tx.from_asset_issuer,
    tx.from_amount,
    tx.from_tx_hash,
    tx.to_network,
    tx.to_address,
    tx.to_asset_code,
    tx.to_asset_issuer,
    tx.to_amount,
    tx.to_tx_hash,
    tx.tx_status,
    tx.tx_fee,
  ];

  // Execute the SQL statement to insert the row
  db.run(insertQuery, params, function (err: any) {
    if (err) {
      console.error("Error inserting row:", err.message);
    } else {
      //   console.log(`Row inserted with row ID ${this.lastID}`);
    }
  });

  // Close the database connection
  db.close((err: any) => {
    if (err) {
      console.error(err.message);
    }
  });
}

// Function to check if a row with specific from_tx_hash exists in the database
export async function checkIfTxExists(fromTxHash: string): Promise<boolean> {
  return new Promise<boolean>((resolve, reject) => {
    // Open the SQLite database
    const db = new sqlite3.Database(
      filepath,
      sqlite3.OPEN_READWRITE,
      (err: any) => {
        if (err) {
          reject(err);
        }
      }
    );

    // SQL statement to check if the row exists
    const query =
      "SELECT COUNT(*) AS count FROM transactions WHERE from_tx_hash = ?";

    // Execute the SQL query to check if the row exists
    db.get(query, [fromTxHash], (err: any, row: any) => {
      if (err) {
        console.error("Error checking row:", err.message);
        reject(err);
      } else {
        const rowExists = row.count > 0;
        resolve(rowExists);
      }
    });

    // Close the database connection
    db.close((err: any) => {
      if (err) {
        console.error(err.message);
      }
    });
  });
}

// Function to retrieve all transactions from the database
export async function getAllTransactions() {
  return new Promise<Tx[]>((resolve, reject) => {
    // Open the SQLite database
    const db = new sqlite3.Database(filepath, sqlite3.OPEN_READWRITE, (err) => {
      if (err) {
        reject(err);
      }
    });

    // SQL query to select all transactions
    const query = "SELECT * FROM transactions";

    // Execute the SQL query to retrieve all transactions
    db.all(query, (err, rows) => {
      if (err) {
        console.error("Error retrieving transactions:", err.message);
        reject(err);
      } else {
        // Convert rows to Tx objects
        const transactions: Tx[] = rows.map((row: any) => ({
          from_network: row.from_network,
          from_address: row.from_address,
          from_asset_code: row.from_asset_code,
          from_asset_issuer: row.from_asset_issuer,
          from_amount: row.from_amount,
          from_tx_hash: row.from_tx_hash,
          to_network: row.to_network,
          to_address: row.to_address,
          to_asset_code: row.to_asset_code,
          to_asset_issuer: row.to_asset_issuer,
          to_amount: row.to_amount,
          to_tx_hash: row.to_tx_hash,
          tx_status: row.tx_status,
          tx_fee: row.tx_fee,
        }));
        resolve(transactions);
      }
    });

    // Close the database connection
    db.close((err: any) => {
      if (err) {
        console.error(err.message);
      }
    });
  });
}
