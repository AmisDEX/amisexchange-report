const AsyncJs = require("async");
const Web3 = require("web3");
const UbiBooks = require("ubitok-jslib/ubi-books.js");
const UbiTokTypes = require("ubitok-jslib/ubi-tok-types.js");
const ZeroClientProvider = require("web3-provider-engine/zero.js");
const { Client, Pool } = require('pg');

let BigNumber = UbiTokTypes.BigNumber;

class Ingester {

  constructor(networkId, dbClient, cpsCallback) {
    this._dbClient = dbClient;
    this._cpsCallback = cpsCallback;
    this._networkId = networkId;
    this._timeout = undefined;
    this._web3 = this._setupWeb3(networkId);
  }

  ingest() {
    let allBooksInfo = UbiBooks.bookInfo;
    AsyncJs.eachOfLimit(allBooksInfo, 1, this._ingestBookCps.bind(this), this._cpsCallback);
  }

  _ingestBookCps(bookInfo, key, cpsCallback) {
    if (bookInfo.networkId !== this._networkId) {
      return cpsCallback(null, false);
    }
    const bookIngester = new BookIngester(this._web3, this._dbClient, bookInfo, cpsCallback);
    return bookIngester.ingest();
  }

  _setupWeb3(networkId) {
    const endpoint = this._getInfuraEndpoint(networkId);
    const web3 = new Web3(
      ZeroClientProvider({
        static: {
          eth_syncing: false,
          web3_clientVersion: "ZeroClientProvider",
        },
        pollingInterval: 4000,
        rpcUrl: endpoint,
        // account mgmt
        getAccounts: (cb) => cb(null, [])
      })
    );
    return web3;
  }

  _getInfuraEndpoint(networkId) {
    // ok, it's trivial to bypass the obfuscation - but please don't, it's against the T&Cs.
    let token = decodeURI("%55%4c%52%35%6e%5a%57%46%77%39%4f%4f%71%67%77%35%58%76%61%42");
    if (networkId === "3") {
      return "https://ropsten.infura.io/" + token;
    } else if (networkId === "1") {
      return "https://mainnet.infura.io/" + token;
    } else if (networkId === "4") {
      return "https://rinkeby.infura.io/" + token;
    } else {
      throw new Error("unknown networkId " + networkId);
    }
  }

}

class BookIngester {

  constructor(web3, dbClient, bookInfo, doneCallback) {
    this._web3 = web3;
    this._dbClient = dbClient;
    this._bookInfo = bookInfo;
    this._doneCallback = doneCallback;
    const BookContract = web3.eth.contract(bookInfo.bookAbiArray);
    this._bookContract = BookContract.at(bookInfo.bookAddress);
    const BaseTokenContract = web3.eth.contract(bookInfo.base.abiArray);
    this._baseToken = BaseTokenContract.at(bookInfo.base.address);
  }

  ingest() {
    console.log("ingesting", this._bookInfo.symbol);
    AsyncJs.series(
      [this._findFromBlock.bind(this), this._findToBlock.bind(this)],
      this._ingestBlocks.bind(this, this._doneCallback)
    );
  }

  _findFromBlock(doneCallback) {
    this._dbClient.query(
      'SELECT last_block_number_ingested FROM book WHERE book_address = LOWER($1)',
      [this._bookInfo.bookAddress],
      (err, res) => {
        if (err) {
          return doneCallback(err, null);
        }
        if (res.rows.length != 1) {
          return doneCallback(new Error("not exactly one row"), null);
        }
        const lastBlockNumber = res.rows[0].last_block_number_ingested;
        return doneCallback(null, lastBlockNumber + 1);
      }
    );
  }

  _findToBlock(doneCallback) {
    this._web3.eth.getBlock("latest", (error, result) => {
      if (error) {
        return doneCallback(error, null);
      }
      const latestBlockNumber = result.number;
      const confirmationsNeeded = 16;
      return doneCallback(error, latestBlockNumber - confirmationsNeeded);
    });
  }

  _ingestBlocks(doneCallback, error, blockRange) {
    if (error) {
      return doneCallback(error, null);
    }
    console.log("ingesting", this._bookInfo.symbol, blockRange);
    const fromBlock = blockRange[0];
    const toBlock = blockRange[1];
    AsyncJs.series([
        this._ingestClientOrderEvents.bind(this, fromBlock, toBlock),
        this._ingestMarketOrderEvents.bind(this, fromBlock, toBlock),
        this._ingestClientPaymentEvents.bind(this, fromBlock, toBlock),
        this._updateBookLastBlockNumberIngested.bind(this, this._bookInfo.bookAddress, fromBlock-1, toBlock),
      ], doneCallback
    );
  }
  
  _updateBookLastBlockNumberIngested(bookAddress, prevLast, newLast, doneCallback) {
    this._dbClient.query(
      'UPDATE book set last_block_number_ingested = $1' +
      'WHERE book_address = LOWER($2) AND last_block_number_ingested = $3',
      [newLast, bookAddress, prevLast],
      doneCallback
    );
  }

  _ingestClientOrderEvents(fromBlock, toBlock, doneCallback) {
    const filter = this._bookContract.ClientOrderEvent({}, {
      fromBlock: fromBlock,
      toBlock: toBlock
    });
    filter.get(this._handleRawClientOrderEvents.bind(this, doneCallback));
  }

  _ingestMarketOrderEvents(fromBlock, toBlock, doneCallback) {
    doneCallback(null, true);
  }

  _ingestClientPaymentEvents(fromBlock, toBlock, doneCallback) {
    doneCallback(null, true);
  }

  _handleRawClientOrderEvents(doneCallback, error, rawClientOrderEvents) {
    if (error) {
      return doneCallback(error, null);
    }
    AsyncJs.mapLimit(rawClientOrderEvents, 3,
      this._decorateOrderEventWithOrder.bind(this),
      this._handleClientOrderEventsDecoratedWithOrders.bind(this, doneCallback));
  }

  _decorateOrderEventWithOrder(rawOrderEvent, doneCallback) {
    this._bookContract.getOrder(rawOrderEvent.args.orderId, (error, result) => {
      if (error) {
        return doneCallback(error, null);
      }
      rawOrderEvent.order = result;
      return doneCallback(error, rawOrderEvent);
    });
  }

  _decorateEventWithBlockTimestamp(event, doneCallback) {
    this._web3.eth.getBlock(event.blockNumber, (error, result) => {
      if (error) {
        return doneCallback(error, null);
      }
      event.blockTimestamp = result.timestamp;
      return doneCallback(error, event);
    });
  }
  
  _handleClientOrderEventsDecoratedWithOrders(doneCallback, error, result) {
    if (error) {
      return doneCallback(error, null);
    }
    const clientOrderEventsDecoratedWithOrders = result;
    AsyncJs.mapLimit(clientOrderEventsDecoratedWithOrders, 3,
      this._decorateEventWithBlockTimestamp.bind(this),
      this._handleClientOrderEventsFullyDecorated.bind(this, doneCallback));
  }

  _handleClientOrderEventsFullyDecorated(doneCallback, error, result) {
    if (error) {
      return doneCallback(error, null);
    }
    const decoratedClientOrderEvents = result;
    const clientOrderEventRows = result.map(r => {
      const row = UbiTokTypes.decodeClientOrderEvent(r);
      row.bookAddress = r.address;
      row.blockTimestamp = new Date(1000 * r.blockTimestamp);
      row.transactionHash = r.transactionHash;
      // TODO - hang on, we oughta make use of r.order too ...
      return row;
    });
    AsyncJs.eachLimit(clientOrderEventRows, 3, this._insertClientOrderEventRow.bind(this), doneCallback);
  }

  _insertClientOrderEventRow(row, doneCallback) {
    const query = {
      text:
        'INSERT INTO client_order_event (' +
        '  book_address,' +
        '  block_timestamp,' +
        '  block_number,' +
        '  transaction_hash,' +
        '  log_index,' +
        '  client_address,' +
        '  client_order_event_type,' +
        '  order_id,' +
        '  max_matches' +
        ') VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
      values: [
        row.bookAddress,
        row.blockTimestamp,
        row.blockNumber,
        row.transactionHash,
        row.logIndex,
        row.client,
        row.clientOrderEventType,
        row.orderId,
        (row.maxMatches === undefined ? undefined : parseInt(row.maxMatches, 10))
      ],
    }
    this._dbClient.query(query, doneCallback);
  }

}

const pool = new Pool();
console.log("starting ingester");
const ingester = new Ingester("1", pool, function (error, result) {
  console.log("ingester finished", "error", error, "result", result);
  pool.end();
  process.exit(error ? 1 : 0);
});
ingester.ingest();
console.log("waiting for ingester");
