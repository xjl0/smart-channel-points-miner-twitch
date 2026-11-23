-- Настройки таймеров движка, которые меняются из админки. Одна строка (id = 1);
-- data — сериализованный protobuf miner.v1.Settings. Нет строки — значения по умолчанию.
CREATE TABLE settings (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    data       BLOB     NOT NULL,
    updated_at DATETIME NOT NULL
);
