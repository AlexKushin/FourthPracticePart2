DROP TABLE IF EXISTS "epicentrRepo".products;
DROP TABLE IF EXISTS "epicentrRepo".availability;
DROP TABLE IF EXISTS "epicentrRepo".stores;
DROP TABLE IF EXISTS "epicentrRepo".productTypes;
CREATE TABLE IF NOT EXISTS "epicentrRepo".stores
(
    id           int,
    storeAddress text,
    PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS "epicentrRepo".productTypes
(
    id          int,
    productType text,
    PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS "epicentrRepo".products
(
    id    int,
    type  int,
    store int,
    name  text,
    PRIMARY KEY ((type), store, id)
);
CREATE TABLE IF NOT EXISTS "epicentrRepo".availability
(
    type     int,
    store    int,
    quantity int,
    PRIMARY KEY ((type), quantity, store)
);