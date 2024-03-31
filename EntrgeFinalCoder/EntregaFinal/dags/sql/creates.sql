CREATE TABLE IF NOT EXISTS  ruthn_cruz09_coderhouse.covidfinal (
    fips VARCHAR(10),
    country VARCHAR(3),
    state VARCHAR(3),
    county VARCHAR(100),
    population NUMERIC,
    lastUpdatedDate TIMESTAMP,
    "actuals.cases" INTEGER, 
    "actuals.deaths" INTEGER, 
    "actuals.icuBeds.capacity" FLOAT,
    "actuals.icuBeds.currentUsageTotal" FLOAT,
    "actuals.icuBeds.currentUsageCovid" FLOAT,
    PRIMARY KEY (fips, lastUpdatedDate) -- Clave primaria compuesta
) DISTSTYLE KEY DISTKEY (fips) SORTKEY (fips, lastUpdatedDate);


