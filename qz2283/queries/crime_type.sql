CREATE TABLE crime_type_lookup AS
SELECT
    arrest_date,
    latitude,
    longitude,
    offense_type,
    CASE
        WHEN offense_type IN (
            'PETIT LARCENY', 'GRAND LARCENY', 'OTHER OFFENSES RELATED TO THEFT',
            'ROBBERY', 'BURGLARY', 'POSSESSION OF STOLEN PROPERTY',
            'GRAND LARCENY OF MOTOR VEHICLE', 'UNAUTHORIZED USE OF A VEHICLE'
        ) THEN 'Property'
        WHEN offense_type IN (
            'ASSAULT 3 & RELATED OFFENSES', 'FELONY ASSAULT',
            'MURDER & NON-NEGL. MANSLAUGHTE', 'RAPE', 'SEX CRIMES',
            'OFFENSES AGAINST THE PERSON'
        ) THEN 'Violent'
        WHEN offense_type = 'DANGEROUS DRUGS' THEN 'Drug'
        WHEN offense_type = 'DANGEROUS WEAPONS' THEN 'Weapons'
        ELSE 'Other'
    END as crime_type
FROM crime_data
WHERE offense_type IN (
    'PETIT LARCENY', 'GRAND LARCENY', 'OTHER OFFENSES RELATED TO THEFT',
    'ROBBERY', 'BURGLARY', 'ASSAULT 3 & RELATED OFFENSES', 'FELONY ASSAULT',
    'DANGEROUS DRUGS', 'DANGEROUS WEAPONS'
);


CREATE TABLE analysis_crime_types_top_stations AS
WITH selected_stations AS (
    SELECT complex_id, stop_name, borough, total_crimes
    FROM analysis_station_crime_ridership_scatter
    ORDER BY total_crimes DESC
    LIMIT 20
),
daily_by_type AS (
    SELECT
        DATE(r.ride_time) as ride_date,
        r.complex_id,
        ss.stop_name,
        ss.borough,
        ss.total_crimes as total_crimes_2024,
        SUM(r.rider_count) as daily_riders,
        SUM(CASE WHEN ct.crime_type = 'Property' THEN 1 ELSE 0 END) as property_crimes,
        SUM(CASE WHEN ct.crime_type = 'Violent' THEN 1 ELSE 0 END) as violent_crimes,
        SUM(CASE WHEN ct.crime_type = 'Drug' THEN 1 ELSE 0 END) as drug_crimes,
        SUM(CASE WHEN ct.crime_type = 'Weapons' THEN 1 ELSE 0 END) as weapons_crimes
    FROM mta_ridership r
    JOIN selected_stations ss ON r.complex_id = ss.complex_id
    JOIN mta_subway_stations s ON r.complex_id = s.complex_id
    LEFT JOIN crime_type_lookup ct ON
        DATE(r.ride_time) = ct.arrest_date
        AND ABS(s.latitude - ct.latitude) < 0.01
        AND ABS(s.longitude - ct.longitude) < 0.01
        AND 111.045 * DEGREES(ACOS(
            COS(RADIANS(ct.latitude)) * COS(RADIANS(s.latitude)) *
            COS(RADIANS(ct.longitude) - RADIANS(s.longitude)) +
            SIN(RADIANS(ct.latitude)) * SIN(RADIANS(s.latitude))
        )) < 0.5
    GROUP BY DATE(r.ride_time), r.complex_id, ss.stop_name, ss.borough, ss.total_crimes
)
SELECT * FROM daily_by_type;

-- Verify 
SELECT * FROM analysis_crime_types_top_stations LIMIT 10;

-- Export
INSERT OVERWRITE DIRECTORY '/user/qz2283_nyu_edu/analysis_results/crime_types_top20'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM analysis_crime_types_top_stations
ORDER BY complex_id, ride_date;