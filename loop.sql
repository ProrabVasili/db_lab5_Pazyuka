CREATE TABLE location_copy AS 
SELECT * FROM location;

SELECT * FROM location_copy;

DO $$
 DECLARE
     c_id   location_copy.city_id%TYPE;
     city_name location_copy.city%TYPE;

 BEGIN
     c_id := 5;
     city_name := 'test_New_Kyiv_';
     FOR counter IN 1..3
         LOOP
            INSERT INTO location_copy (city_id, city)
            VALUES (c_id + counter, city_name || counter);
         END LOOP;
 END;
 $$
 

DROP TABLE location_copy;