import EventSource from "eventsource";
import amqp from "amqplib";
import Web3 from "web3";
import dotenv from "dotenv";
import {
  getAssetIssuer,
  extractOpsData,
  getAssetCodeByIssuer,
  extractTransferredAmount,
  extractMemoData,
} from "./utils";

dotenv.config();

// Define environment variables
const {
  ETHEREUM_VAULT_ADDRESS,
  STELLAR_VAULT_ADDRESS,
  ETHEREUM_WS,
  NETWORK_TYPE,
  STELLAR_ES,
  CLOUDAMQP_URL,
} = process.env;

// Define types
type Tx = {
  // from network
  from_network: string;
  from_address: string;
  from_asset_code: string;
  from_asset_issuer: string;
  from_amount: string | number;
  from_tx_hash: string;
  // to network
  to_network: string;
  to_address: string;
  to_asset_code: string;
  to_asset_issuer: string;
  to_amount?: string | number;
  to_tx_hash?: string;
};

// Function to create Stellar EventSource connection
async function createStellarConnection(channel: amqp.Channel) {
  const stellarConn = new EventSource(
    `${STELLAR_ES}/accounts/${STELLAR_VAULT_ADDRESS}/payments`
  );

  stellarConn.onmessage = async (message) => {
    try {
      const tx = message.data ? JSON.parse(message.data) : message;

      if (tx?.type === "payment") {
        const tx_response = await fetch(tx?._links.transaction.href);
        const ops_response = await fetch(
          tx?._links.transaction.href + `/operations`
        );
        if (tx_response.ok && ops_response.ok) {
          const tx_data = await tx_response.json();
          const ops_data = await ops_response.json();

          const {
            asset_code: from_asset_code,
            asset_issuer: from_asset_issuer,
            from,
            amount: from_amount,
          } = tx;
          const from_tx_hash = tx_data?.hash;
          const from_network = "STELLAR";
          const op = extractOpsData(ops_data._embedded.records);

          const stellarTx: Tx = {
            from_network,
            from_asset_code,
            from_asset_issuer,
            from_address: from,
            from_amount,
            from_tx_hash,
            ...op,
          };

          // Send the tx to the processing queue
          channel.sendToQueue(
            "txQueue",
            Buffer.from(JSON.stringify(stellarTx)),
            { persistent: true }
          );
          console.log("StellarTx sent to queue:", stellarTx);
        } else {
          console.log("Tx type is not payment");
        }
      }
    } catch (error) {
      console.log("Error processing Stellar message:", error);
    }
  };

  stellarConn.onerror = (error) => {
    console.error("An error occurred with Stellar connection!", error);
    if (error.status === 404) {
      setTimeout(createStellarConnection, 60000, channel); // Retry connection after 60 seconds
    }
  };
}

// Function to create Ethereum WebSocket connection
async function createEthereumConnection(channel: amqp.Channel) {
  const web3 = new Web3(ETHEREUM_WS);
  const subscription = await web3.eth.subscribe("logs", {
    address: null,
    topics: [
      web3.utils.sha3("Transfer(address,address,uint256)"),
      null,
      web3.eth.abi.encodeParameter("address", ETHEREUM_VAULT_ADDRESS),
    ],
  });
  subscription.on("data", async (data: any) => {
    const tx = await web3.eth.getTransaction(data?.transactionHash);

    const input = web3.utils.hexToUtf8(tx.input);
    const memo = extractMemoData(input);

    const from_asset_code = getAssetCodeByIssuer(tx.to);
    const from_amount = extractTransferredAmount(tx);

    const ethereumTx: Tx = {
      from_network: "ETH",
      from_address: tx?.from,
      from_asset_code,
      from_asset_issuer: tx.to,
      from_amount,
      from_tx_hash: tx.hash,
      ...memo,
    };

    // Send the tx to the processing queue
    channel.sendToQueue("txQueue", Buffer.from(JSON.stringify(ethereumTx)), {
      persistent: true,
    });
    console.log("EthereumTx sent to queue:", ethereumTx);
  });

  subscription.on("error", (error: any) => console.log(error));
}

async function init() {
  let connection: amqp.Connection;
  const maxRetries = 5;
  let retryCount = 0;
  const retryDelay = 10000; // 10 seconds

  const connectToRabbitMQ = async () => {
    try {
      // Connect to RabbitMQ
      connection = await amqp.connect(CLOUDAMQP_URL);
      const channel = await connection.createChannel();

      // Define the queue
      const queueName = "txQueue";
      await channel.assertQueue(queueName, { durable: true });

      // Create network connections
      await Promise.all([
        createStellarConnection(channel),
        createEthereumConnection(channel),
      ]);
    } catch (error) {
      console.error("Error connecting to RabbitMQ:", error);
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
