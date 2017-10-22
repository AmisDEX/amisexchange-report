
CREATE TABLE book (
  book_address               varchar(42) not null,
  last_block_number_ingested int not null,

  PRIMARY KEY (book_address)
);

CREATE TABLE client_order_event (
  book_address               varchar(42) not null,
  block_timestamp            timestamptz not null,
  block_number               int not null,
  transaction_index          int not null,
  -- careful - parity/geth differences with log_index?
  log_index                  int not null,
  client_address             varchar(42) not null,
  client_order_event_type    varchar not null,
    CHECK (client_order_event_type in ('Create', 'Continue', 'Cancel')),
  order_id                   varchar not null,
  max_matches                int not null,

  PRIMARY KEY (book_address, block_number, log_index),
  FOREIGN KEY (book_address) REFERENCES book (book_address)
);

CREATE INDEX client_order_event_ix1 ON client_order_event (book_address, block_timestamp);
CREATE INDEX client_order_event_ix2 ON client_order_event (client_address);
