CREATE TABLE IF NOT EXISTS user_airports(
	email_utente VARCHAR(255),
	icao_aeroporto VARCHAR(6),
	high_value INT NULL,
	low_value INT NULL,
	PRIMARY KEY(email_utente, icao_aeroporto),
	CHECK (high_value IS NULL OR low_value IS NULL OR high_value > low_value)
);

CREATE TABLE IF NOT EXISTS flight(
	id INT AUTO_INCREMENT PRIMARY KEY,
	icao_aereo VARCHAR(6) NOT NULL,
    first_seen INT,
    aeroporto_partenza VARCHAR(4) NOT NULL,
    last_seen INT,
    aeroporto_arrivo VARCHAR(4) NOT NULL,
    CONSTRAINT unique_flight UNIQUE (icao_aereo, first_seen, aeroporto_partenza)
);