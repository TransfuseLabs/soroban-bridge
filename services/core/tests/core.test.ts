// Import necessary modules and functions for testing
import request from "supertest";
import { Tx, swap } from "../src/core";
import { app } from "../src/core";
import { insertTx, getAllTransactions } from "../src/db";

describe("Swap Function Tests", () => {
  test("should swap from ETH to Stellar", async () => {
    // Mock data for ETH to Stellar swap
    const data: Tx = {
      from_network: "ETH",
      from_address: "ETH_ADDRESS",
      from_asset_code: "ETH_CODE",
      from_asset_issuer: "ETH_ISSUER",
      from_amount: 10,
      from_tx_hash: "ETH_TX_HASH",
      to_network: "STELLAR",
      to_address: "STELLAR_ADDRESS",
      to_asset_code: "STELLAR_CODE",
      to_asset_issuer: "STELLAR_ISSUER",
      tx_status: "swap",
    };

    console.log(data.tx_status); // This should not throw an error

    // Mock the insertTx function to avoid actual database insertion
    (insertTx as any).mockReturnValueOnce(Promise.resolve());

    // Mock the getAllTransactions function to avoid actual database query
    // Mock the getAllTransactions function to avoid actual database query
    (getAllTransactions as any).mockReturnValueOnce(Promise.resolve([]));

    // Call the swap function with mock data
    await swap(data);

    // Assert that the transaction status is 'swap'
    expect(data.tx_status).toEqual("swap");
  });

  test("should handle error for failed swap", async () => {
    // Mock data for ETH to Stellar swap
    const data: Tx = {
      from_network: "ETH",
      from_address: "ETH_ADDRESS",
      from_asset_code: "ETH_CODE",
      from_asset_issuer: "ETH_ISSUER",
      from_amount: 10,
      from_tx_hash: "ETH_TX_HASH",
      to_network: "STELLAR",
      to_address: "STELLAR_ADDRESS",
      to_asset_code: "STELLAR_CODE",
      to_asset_issuer: "STELLAR_ISSUER",
      tx_status: "failed swap",
    };

    // Mocking a function that will throw an error to simulate failure
    (insertTx as any).mockImplementationOnce(() => {
      throw new Error("Failed to insert transaction");
    });

    // Call the swap function with mock data
    await swap(data);

    // Assert that the transaction status is 'failed swap'
    expect(data.tx_status).toEqual("failed swap");
  });
});

describe("Express App Endpoint Tests", () => {
  test("GET /status should return status 200 and status object", async () => {
    const res = await request(app).get("/status");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("status", "live");
    expect(res.body.tokens).toHaveLength(3);
  });

  test("GET /transactions should return status 200 and array of transactions", async () => {
    // Mock the database response for transactions
    const mockTransactions = [{}, {}];
    (getAllTransactions as any).mockReturnValueOnce(
      Promise.resolve(mockTransactions)
    );

    const res = await request(app).get("/transactions");
    expect(res.status).toBe(200);
    expect(res.body).toEqual(mockTransactions);
  });
});
