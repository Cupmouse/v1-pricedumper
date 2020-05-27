#ifndef __COMMON_H__
#define __COMMON_H__

const char* DEF_TICKER_TABLE = "CREATE TABLE IF NOT EXISTS %s (\\
    timestamp INTEGER NOT NULL,\\
    best_bid REAL NOT NULL,\\
    best_ask REAL NOT NULL',\\
    best_bid_size REAL NOT NULL',\\
    best_ask_size REAL NOT NULL',\\
    total_bid_depth REAL NOT NULL',\\
    total_ask_depth REAL NOT NULL',\\
    last_traded_price REAL NOT NULL',\\
    volume REAL NOT NULL',\\
    volume_by_product REAL NOT NULL',\\
    )";

#endif