-- 讓重新建立 schema 時不會因為舊資料表而失敗
DROP TABLE IF EXISTS stop_events CASCADE;
DROP TABLE IF EXISTS trips CASCADE;
DROP TABLE IF EXISTS line_stops CASCADE;
DROP TABLE IF EXISTS stops CASCADE;
DROP TABLE IF EXISTS lines CASCADE;


CREATE TABLE lines (
    line_id                    SERIAL PRIMARY KEY,  -- or use line_name as natural key?
    line_name     VARCHAR(50)  NOT NULL UNIQUE,
    vehicle_type  VARCHAR(10)  NOT NULL CHECK (vehicle_type IN ('rail', 'bus'))
);

CREATE TABLE stops (
    -- Your design here
    stop_id                     SERIAL PRIMARY KEY,
    stop_name  VARCHAR(50)      NOT NULL,
    latitude   DECIMAL(10, 6),
    longitude  DECIMAL(10, 6),

    UNIQUE (stop_name, latitude, longitude)
);

CREATE TABLE line_stops (
    -- Your design here
    -- Must handle: line_id, stop_id, sequence_number, time_offset_minutes
    line_id              INTEGER  NOT NULL,  -- 對應 Lines.id
    stop_id              INTEGER  NOT NULL,  -- 對應 Stops.id
    sequence_number      INTEGER  NOT NULL,  -- 第幾站（1,2,3,...）
    time_offset_minutes  INTEGER  NOT NULL,  -- 自起點出發累積分鐘數（>=0）

    -- 1) 主鍵：同一條路線的「站序」必須唯一
    PRIMARY KEY (line_id, sequence_number),

    -- 3) 外鍵
    FOREIGN KEY (line_id) REFERENCES lines(line_id) ON DELETE CASCADE,
    FOREIGN KEY (stop_id) REFERENCES stops(stop_id) ON DELETE RESTRICT,

    -- 4) 合理性檢查
    CHECK (sequence_number >= 1),
    CHECK (time_offset_minutes >= 0)
);

-- Continue for trips and stop_events
CREATE TABLE trips (
    trip_id         VARCHAR(50)  PRIMARY KEY,
    line_id         INTEGER      NOT NULL,
    departure_time  TIMESTAMP    NOT NULL, 
    vehicle_id      VARCHAR(50)  NOT NULL,

    -- 外鍵約束，確保每個 trip 對應到一條已存在的 line
    FOREIGN KEY (line_id) REFERENCES lines(line_id) ON DELETE CASCADE
);

CREATE TABLE stop_events (
    event_id                     SERIAL PRIMARY KEY,  -- 每個停靠事件的唯一識別碼
    trip_id         VARCHAR(50)  NOT NULL,            -- 外鍵：此事件屬於哪一個 Trip
    stop_id         INTEGER      NOT NULL,            -- 外鍵：在哪一個 Stop 發生
    scheduled_time  TIMESTAMP    NOT NULL,            -- 原本預定到站時間
    actual_time     TIMESTAMP    NOT NULL,            -- 實際到站時間（可為 NULL 表示尚未到）
    passengers_on   INTEGER      NOT NULL,            -- 上車人數
    passengers_off  INTEGER      NOT NULL,            -- 下車人數

    -- 外鍵約束
    FOREIGN KEY (trip_id) REFERENCES trips(trip_id) ON DELETE CASCADE,
    FOREIGN KEY (stop_id) REFERENCES stops(stop_id) ON DELETE RESTRICT,

    -- 合理性檢查
    CHECK (passengers_on >= 0),
    CHECK (passengers_off >= 0)
);