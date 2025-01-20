# validator
Offical repo for the Ionize asset bridge. Please note that this is a demo version, which currently supports:

- Bi-directional swaps between `Ethereum Sepolia` and `Stellar testnet`
- Assets that can be swapped:
    - Ethereum Sepolia: USDC, USDT
    - Stellar testnet: USDC

## Requirements
- Supported architectures: Linux arm64 & amd64
- Memory (RAM): >16 GB  (if you want to run an Ethereum node locally)
- Storage (Disk space): >80 GB (if you want to run an Ethereum node locally)
- Docker Compose

## Setup

### Step 1: Clone the repo
```
git clone git@github.com:ionizedman/validator.git &&

cd validator
```

### Step 2: Configure your validator 
```cmd
touch .env && cp .env.base .env
```
Add the vault addresses and keys in the command below, then run it in your terminal.
```cmd
{
    # Add Ethereum vault address
    echo "ETHEREUM_VAULT_ADDRESS=YOUR_ETHEREUM_ADDRESS"
    
    # Add Ethereum vault private key
    echo "ETHEREUM_VAULT_PRIVATE_KEY=YOUR_ETHEREUM_PRIVATE_KEY"

    # Add Stellar vault address
    echo "STELLAR_VAULT_ADDRESS=YOUR_STELLAR_ADDRESS"
    
    # Add Stellar vault secret key
    echo "STELLAR_VAULT_SECRET_KEY=YOUR_STELLAR_SECRET_KEY"
    
    cat .env
} > .env.tmp && mv .env.tmp .env
```

### Step 3 (Optional): Use a remote Ethereum node
Although it is important to run your own ethereum node, however, if you prefer to rely on a remote provider (e.g. Infura, QuickNode), please run this in your terminal:

```ts
NEW_ETHEREUM_HTTP="<Add your provider's HTTP endpoint URL here>"
NEW_ETHEREUM_WS="<Add your provider's Websocket URL here>"

awk -v new_ethereum_http="$NEW_ETHEREUM_HTTP" -v new_ethereum_ws="$NEW_ETHEREUM_WS" '
BEGIN {
    FS="="
    OFS="="
}
$1 == "ETHEREUM_HTTP" {
    $2 = new_ethereum_http
}
$1 == "ETHEREUM_WS" {
    $2 = new_ethereum_ws
}
{
    print
}' .env > .env.tmp && mv .env.tmp .env
```

### Step 4: Run the validator
```
docker compose -p ionize up -d
```
**Note: It takes some time to syc up on both networks. You can monitor progress by accessing your docker container logs.**


Once syncing is complete on Stellar and Ethereum networks, You can access the Ionize service via:

- REST API endpoints:
    - GET status: `http://localhost:8080/status`
    - GET transactions: `http://localhost:8080/transactions`

- Websocket connection:
    - Stream live transactions: `ws://localhost:3000`

