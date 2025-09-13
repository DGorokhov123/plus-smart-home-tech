DROP TABLE IF EXISTS scenario_conditions;
DROP TABLE IF EXISTS scenario_actions;

DROP TABLE IF EXISTS actions;
DROP TABLE IF EXISTS conditions;

DROP TABLE IF EXISTS scenarios;
DROP TABLE IF EXISTS sensors;



CREATE TABLE IF NOT EXISTS sensors (
    id VARCHAR PRIMARY KEY,
    hub_id VARCHAR NOT NULL,
    type VARCHAR NOT NULL
);


CREATE TABLE IF NOT EXISTS scenarios (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hub_id VARCHAR,
    name VARCHAR,
    UNIQUE(hub_id, name)
);

CREATE TABLE IF NOT EXISTS conditions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scenario_id BIGINT REFERENCES scenarios(id),
    sensor_id VARCHAR REFERENCES sensors(id),
    type VARCHAR,
    operation VARCHAR,
    "value" INTEGER
);

CREATE TABLE IF NOT EXISTS actions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scenario_id BIGINT REFERENCES scenarios(id),
    sensor_id VARCHAR REFERENCES sensors(id),
    type VARCHAR,
    "value" INTEGER
);
