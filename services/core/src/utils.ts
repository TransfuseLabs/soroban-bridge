import { Web3 } from "web3";
import { StrKey } from "stellar-sdk";
import { Server } from "stellar-sdk/lib/horizon";
import dotenv from "dotenv";
//@ts-ignore
import abi from "erc-20-abi";
dotenv.config();
const endpointUrl = process.env.ETHEREUM_HTTP;
const httpProvider = new Web3.providers.HttpProvider(process.env.ETHEREUM_HTTP);
const web3Client = new Web3(httpProvider);

export async function getEthTokenBalance(
  tokenAddress: string,
  walletAddress: string
) {
  const contract = new web3Client.eth.Contract(abi, tokenAddress);
  const result = await contract.methods.balanceOf(walletAddress).call();

  const resultInEther = web3Client.utils.fromWei(result, "wei");
  return Number(resultInEther) / 10 ** 6;
}

export async function getStellarTokenBalance(
  address: string,
  assetIssuer: string
): Promise<string | null> {
  const server = new Server(process.env.STELLAR_ES, {
    allowHttp: true,
  }); // Use the Horizon server URL

  try {
    // Check if the address is a valid Stellar public key
    if (!StrKey.isValidEd25519PublicKey(address)) {
      throw new Error("Invalid Stellar address");
    }

    // Load the account data for the address
    const account = await server.loadAccount(address);

    // Find the balance for the specified asset issuer
    const balance = account.balances.find((asset) => {
      return (
        (asset.asset_type === "credit_alphanum4" ||
          asset.asset_type === "credit_alphanum12") &&
        asset.asset_issuer === assetIssuer
      );
    });

    return balance ? balance.balance : null;
  } catch (error) {
    console.error("Error fetching Stellar token balance:", error);
    throw error;
  }
}
