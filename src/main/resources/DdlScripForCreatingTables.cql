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
    PRIMARY KEY (productType)
);
CREATE TABLE IF NOT EXISTS "epicentrRepo".products
(
    id    uuid,
    type  int,
    name  text,
    PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS "epicentrRepo".delivery
(
    id    uuid,
    deliveryDateTime timestamp,
    type  int,
    store int,
    PRIMARY KEY ((store),type,id, deliveryDateTime)
);
CREATE TABLE IF NOT EXISTS "epicentrRepo".availability
(
    type     int,
    store    int,
    quantity int,
    PRIMARY KEY ((type), quantity, store)
);