import express from "express";
import cors from "cors";
import WebSocket from "ws";
import httpModule from "http";
import amqp from "amqplib";
import { Core } from "@quicknode/sdk";
import { sepolia, mainnet } from "viem/chains";
import { createWalletClient, http } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import * as StellarSdk from "stellar-sdk";
import { Server } from "stellar-sdk/lib/horizon";
import dotenv from "dotenv";
import {
  checkIfTxExists,
  createTable,
  getAllTransactions,
  insertTx,
} from "./db";
import { getEthTokenBalance, getStellarTokenBalance } from "./utils";
//@ts-ignore
import abi from "erc-20-abi";

dotenv.config();

export const app = express();
const PORT = 8080;
const WS_PORT = 3000;

const ETHEREUM_VAULT_ADDRESS = process.env.ETHEREUM_VAULT_ADDRESS || "";
const STELLAR_VAULT_ADDRESS = process.env.STELLAR_VAULT_ADDRESS || "";
const ETHEREUM_VAULT_PRIVATE_KEY = process.env.ETHEREUM_VAULT_PRIVATE_KEY || "";
const STELLAR_VAULT_SECRET_KEY = process.env.STELLAR_VAULT_SECRET_KEY || "";
const ETHEREUM_HTTP = process.env.ETHEREUM_HTTP || "";
const NETWORK_TYPE = process.env.NETWORK_TYPE || "";
const MIN_SWAP = process.env.MIN_SWAP || "";
const MAX_SWAP = process.env.MAX_SWAP || "";
const SWAP_FEE_ETH = process.env.SWAP_FEE_ETH || "";
const SWAP_FEE_STELLAR = process.env.SWAP_FEE_STELLAR || "";
const STELLAR_ES = process.env.STELLAR_ES || "";
const CLOUDAMQP_URL = process.env.CLOUDAMQP_URL || "";

// Define transaction status
type TxStatus = "liquidity added" | "swap" | "failed swap";

// Define transaction object type
export type Tx = {
  from_network: string;
  from_address: string;
  from_asset_code: string;
  from_asset_issuer: string;
  from_amount: string | number;
  from_tx_hash: string;
  to_network: string;
  to_address: string;
  to_asset_code: string;
  to_asset_issuer: string;
  to_amount?: string | number;
  to_tx_hash?: string;
  tx_status?: TxStatus;
  tx_fee?: number;
};

// Establish WebSocket server
const wss = new WebSocket.Server({ port: WS_PORT });

// Initialize Express app
app.use(express.json());

// Allow requests from all origins
app.use(cors());

// Status endpoint
app.get("/status", async (req, res) => {
  const USDC_ETH =
    NETWORK_TYPE === "testnet"
      ? "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238"
      : "";
  const USDT_ETH =
    NETWORK_TYPE === "testnet"
      ? "0xaA8E23Fb1079EA71e0a56F48a2aA51851D8433D0"
      : "";
  const USDC_STELLAR =
    NETWORK_TYPE === "testnet"
      ? "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
      : "";

  try {
    // Fetch token balances
    const bal_usdt_eth = await getEthTokenBalance(
      USDT_ETH,
      ETHEREUM_VAULT_ADDRESS
    );
    const bal_usdc_eth = await getEthTokenBalance(
      USDC_ETH,
      ETHEREUM_VAULT_ADDRESS
    );
    const bal_usdc_stellar = await getStellarTokenBalance(
      STELLAR_VAULT_ADDRESS,
      USDC_STELLAR
    );

    // Construct status object
    const status = {
      status: "live",
      tokens: [
        {
          id: "USDT.ETH",
          issuerAddress: USDT_ETH,
          vaultAddress: ETHEREUM_VAULT_ADDRESS,
          liquidity: bal_usdt_eth,
          minInput: MIN_SWAP,
          maxInput: MAX_SWAP,
          minOutput: MIN_SWAP,
          maxOutput: MAX_SWAP,
          txFee: SWAP_FEE_ETH,
        },
        {
          id: "USDC.ETH",
          issuerAddress: USDC_ETH,
          vaultAddress: ETHEREUM_VAULT_ADDRESS,
          liquidity: bal_usdc_eth,
          minInput: MIN_SWAP,
          maxInput: MAX_SWAP,
          minOutput: MIN_SWAP,
          maxOutput: MAX_SWAP,
          txFee: SWAP_FEE_ETH,
        },
        {
          id: "USDC.STELLAR",
          issuerAddress: USDC_STELLAR,
          vaultAddress: STELLAR_VAULT_ADDRESS,
          liquidity: bal_usdc_stellar,
          minInput: MIN_SWAP,
          maxInput: MAX_SWAP,
          minOutput: MIN_SWAP,
          maxOutput: MAX_SWAP,
          txFee: SWAP_FEE_STELLAR,
        },
      ],
    };

    // Send status response
    res.status(200).json(status);
  } catch (error) {
    console.error("Error processing transaction:", error);
    res.status(500).send("Error processing transaction");
  }
});

// Transactions endpoint
app.get("/transactions", async (req, res) => {
  try {
    // Fetch all transactions
    const txs = await getAllTransactions();
    res.status(200).json(txs);
  } catch (error) {
    console.error("Error processing transaction:", error);
    res.status(500).send("Error processing transaction");
  }
});

// Swap function
export async function swap(data: Tx) {
  // Check for duplicate transaction
  const isTxDuplicate = await checkIfTxExists(data.from_tx_hash);
  if (isTxDuplicate) {
    return;
  }

  // TODO: Convert amount based on real-time exchange rate
  let converted_to_amount = data.from_amount;
  const to_amount =  (Number(converted_to_amount) - Number(SWAP_FEE_STELLAR)) * 0.99;
  // Validate transaction parameters
  if (
    data.to_address &&
    data.to_asset_issuer &&
    data.to_asset_code &&
    data.from_amount &&
    Number(converted_to_amount) >= Number(MIN_SWAP) &&
    Number(converted_to_amount) <= Number(MAX_SWAP)
  ) {
    // Perform ETH to Stellar swap
    if (data.to_network === "STELLAR") {
      // Stellar transaction logic
      try {
        // Initialize Stellar server
        const server = new Server(STELLAR_ES, { allowHttp: true });

        // Load source account
        const sourceKeys = StellarSdk.Keypair.fromSecret(
          STELLAR_VAULT_SECRET_KEY
        );
        const sourceAccount = await server.loadAccount(sourceKeys.publicKey());

        // Build Stellar transaction
        const stellarTransaction = new StellarSdk.TransactionBuilder(
          sourceAccount,
          {
            fee: StellarSdk.BASE_FEE,
            networkPassphrase:
              NETWORK_TYPE === "testnet"
                ? StellarSdk.Networks.TESTNET
                : StellarSdk.Networks.PUBLIC,
          }
        )
          .addOperation(
            StellarSdk.Operation.payment({
              destination: data.to_address,
              asset: new StellarSdk.Asset(
                data.to_asset_code,
                data.to_asset_issuer
              ),
              amount: (
                to_amount
              ).toString(),
            })
          )
          .setTimeout(180)
          .build();

        // Sign and submit Stellar transaction
        stellarTransaction.sign(sourceKeys);
        const result = await server.submitTransaction(stellarTransaction);

        // Update transaction data
        data.tx_status = "swap";
        data.to_tx_hash = result?.hash;
        data.tx_fee = Number(SWAP_FEE_STELLAR); // TODO: use real-time fee pricing
        data.to_amount = to_amount;
      } catch (error) {
        data.tx_status = "failed swap";
        console.error("Error performing Stellar transaction:", error);
      }
    }

    // Perform Stellar to ETH swap
    if (data.to_network === "ETH") {
      // Ethereum transaction logic
      try {
        // Convert Ethereum private key to account object
        const account = privateKeyToAccount(`0x${ETHEREUM_VAULT_PRIVATE_KEY}`);

        // Initialize Ethereum clients
        const core = new Core({ endpointUrl: ETHEREUM_HTTP });
        const walletClient = createWalletClient({
          account,
          chain: NETWORK_TYPE === "testnet" ? sepolia : mainnet,
          transport: http(core.endpointUrl),
        });

        // Simulate Ethereum contract call
        const { request }: any = await core.client.simulateContract({
          address: data.to_asset_issuer,
          abi: abi,
          functionName: "transfer",
          args: [
            data.to_address,
            BigInt(
              (to_amount) * 10 ** 6
            ),
          ],
          account,
        });

        // Submit Ethereum contract call
        const response = await walletClient.writeContract(request);

        // Update transaction data
        data.tx_status = "swap";
        data.to_tx_hash = response;
        data.tx_fee = Number(SWAP_FEE_ETH); // TODO: use real-time fee pricing
        data.to_amount = to_amount;
      } catch (error) {
        data.tx_status = "failed swap";
        console.error("Error performing Ethereum transaction:", error);
      }
    }
  } else {
    data.tx_status = "liquidity added";
  }

  // Insert transaction into database
  insertTx(data);

  // Broadcast transaction to WebSocket clients
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
}

// Initialize RabbitMQ connection and start server
async function init() {
  let connection: amqp.Connection;
  let retryCount = 0;
  const maxRetries = 5;
  const retryDelay = 10000; // 10 seconds

  const connectToRabbitMQ = async () => {
    try {
      // Connect to RabbitMQ
      connection = await amqp.connect(CLOUDAMQP_URL);
      const channel = await connection.createChannel();

      // Declare the queue to consume from
      const queueName = "txQueue";
      await channel.assertQueue(queueName, { durable: true });

      // Consume messages from the queue
      channel.consume(
        queueName,
        async (msg) => {
          const tx = JSON.parse(msg.content.toString());
          await swap(tx);

          // Acknowledge the message
          channel.ack(msg);
        },
        { noAck: false }
      );

      // Setup database table if it does not exist
      createTable();

      // Create HTTP server
      const server = httpModule.createServer(app);

      // Start WebSocket server
      wss.on("connection", (ws) => {
        console.log("WebSocket client connected");

        ws.on("message", (message) => {
          console.log("Received message:", message);
        });

        ws.on("close", () => {
          console.log("WebSocket client disconnected");
        });
      });

      // Start HTTP server
      server.listen(PORT, () => {
        console.log(`Server is running on port ${PORT}`);
      });
    } catch (error) {
      console.error("Error:", error);
      if (retryCount < maxRetries) {
        console.log(
          `Retrying connection to RabbitMQ in ${retryDelay / 1000} seconds...`
        );
        retryCount++;
        setTimeout(connectToRabbitMQ, retryDelay);
      } else {
        console.error("Max retry attempts reached. Exiting...");
        process.exit(1); // Exit the process if max retry attempts reached
      }
    }
  };

  // Start initial connection attempt
  connectToRabbitMQ();
}

// Initialize the application
init();
