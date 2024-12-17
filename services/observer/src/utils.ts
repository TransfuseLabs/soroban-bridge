import abiDecoder from "abi-decoder";
//@ts-ignore
import abi from "erc-20-abi";
import dotenv from "dotenv";
dotenv.config();

const NETWORK_TYPE = process.env.NETWORK_TYPE;

// Define the type for the JSON structure
type Network = {
  networks: {
    [network: string]: {
      [asset: string]: {
        asset_code: string;
        asset_issuer: string;
      };
    };
  };
};

const tokensJson: Network = {
  networks: {
    ETH: {
      USDC: {
        asset_code: "USDC",
        asset_issuer:
          NETWORK_TYPE === "testnet"
            ? "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238"
            : "",
      },
      USDT: {
        asset_code: "USDT",
        asset_issuer:
          NETWORK_TYPE === "testnet"
            ? "0xaA8E23Fb1079EA71e0a56F48a2aA51851D8433D0"
            : "",
      },
    },
    STELLAR: {
      USDC: {
        asset_code: "USDC",
        asset_issuer:
          NETWORK_TYPE === "testnet"
            ? "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
            : "",
      },
    },
  },
};

// Function to find the network for a given asset
export function findNetworkForAsset(
  asset_code: string,
  asset_issuer: string
): string | null {
  const json: Network = tokensJson;

  for (const network in json.networks) {
    if (Object.prototype.hasOwnProperty.call(json.networks, network)) {
      const assets = json.networks[network];
      for (const asset in assets) {
        if (Object.prototype.hasOwnProperty.call(assets, asset)) {
          const { asset_code: code, asset_issuer: issuer } = assets[asset];
          if (code === asset_code && issuer === asset_issuer) {
            return network;
          }
        }
      }
    }
  }
  return null;
}

export function getAssetIssuer(
  asset_code: string,
  network: string
): string | null {
  const networkData = tokensJson.networks[network];
  if (networkData && networkData[asset_code]) {
    return networkData[asset_code].asset_issuer;
  } else {
    return null;
  }
}

type ExtractedData = {
  to_asset_code: string;
  to_asset_issuer: string;
  to_network: string;
  to_address: string;
};

export function extractOpsData(ops_data: any[]): ExtractedData | null {
  const manageDataRecord = ops_data.find(
    (record) => record.type === "manage_data" && record.name === "ionize"
  );

  let pattern: string = !!manageDataRecord
    ? base64ToText(manageDataRecord?.value)
    : "";

  const regex = /^([^.:]+)\.([^.:]+):(.+)$/;
  const match = !!pattern ? pattern.match(regex) : [null, null, null, null];

  if (!match || match.length !== 4) {
    console.error("Invalid input pattern");
    return null;
  }

  const [, to_asset_code, to_network, to_address] = match;
  const to_asset_issuer = getAssetIssuer(to_asset_code, to_network);
  return {
    to_asset_code,
    to_asset_issuer,
    to_network,
    to_address,
  };
}

export function extractMemoData(memo: string): ExtractedData | null {
  const pattern: string = findMemoInEthTxData(memo);
  const regex = /^([^.:]+)\.([^.:]+):(.+)$/;
  const match = !!pattern ? pattern.match(regex) : [null, null, null, null];

  if (!match || match.length !== 4) {
    console.error("Invalid input pattern");
    return null;
  }

  const [, to_asset_code, to_network, to_address] = match;
  const to_asset_issuer = getAssetIssuer(to_asset_code, to_network);
  return {
    to_asset_code,
    to_asset_issuer,
    to_network,
    to_address,
  };
}

export function getAssetCodeByIssuer(assetIssuer: string): string | null {
  const upperCaseIssuer = assetIssuer.toUpperCase(); // Convert the input to uppercase
  for (const network in tokensJson.networks) {
    const assets = tokensJson.networks[network];
    for (const asset in assets) {
      const currentIssuer = assets[asset].asset_issuer.toUpperCase(); // Convert the asset issuer to uppercase for comparison
      if (currentIssuer === upperCaseIssuer) {
        // Compare with the uppercase version
        return assets[asset].asset_code;
      }
    }
  }
  return null; // Asset issuer not found
}

export function extractTransferredAmount(txData: any): number | null {
  // Decode the data field to get the function signature and the parameters
  const data = txData.data;

  // Extract the transferred amount from the data
  abiDecoder.addABI(abi);

  const decoded = abiDecoder.decodeMethod(data);
  const amount = decoded.params.find(
    (amount: any) => amount.name === "amount"
  ).value;
  console.log(!!amount ? amount / 1000000 : 0)
  return !!amount ? amount / 1000000 : 0;
}

function base64ToText(base64: string): string | undefined {
  try {
    // Convert Base64 to Buffer
    const buffer = Buffer.from(base64, "base64");

    // Convert Buffer to string using UTF-8 encoding
    const text = buffer.toString("utf-8");

    return text;
  } catch (error) {
    console.error("Error converting Base64 to text:", error);
    return undefined;
  }
}

function findMemoInEthTxData(input: string) {
  // Find the index of the substring "IONIZE:"
  const startIndex = input.indexOf("IONIZE:");

  // If "IONIZE:" is found, extract the substring starting from the next character
  if (startIndex !== -1) {
    const extractedSubstring = input.substring(startIndex + 7); // 7 is the length of "IONIZE:"
    return extractedSubstring;
  } else {
    // If "IONIZE:" is not found, return null or handle accordingly
    return null;
  }
}
