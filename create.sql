CREATE TABLE cuisine
(
  cuisine_id    SERIAL PRIMARY KEY,
  type_cuisine  VARCHAR(30) NOT NULL
);

CREATE TABLE location
(
  city_id    SERIAL PRIMARY KEY,
  city       VARCHAR(30) NOT NULL
);

CREATE TABLE restaurant
(
  restaurant_id    SERIAL PRIMARY KEY,
  restaurant_name  VARCHAR(30) NOT NULL,
  rating           FLOAT NOT NULL,
  city_id          INT NOT NULL,
  FOREIGN KEY (city_id) REFERENCES location(city_id)
);

CREATE TABLE restaurant_cuisine
(
  restaurant_id  INT NOT NULL,
  cuisine_id     INT NOT NULL,
  PRIMARY KEY (restaurant_id, cuisine_id),
  FOREIGN KEY (restaurant_id) REFERENCES restaurant(restaurant_id),
  FOREIGN KEY (cuisine_id) REFERENCES cuisine(cuisine_id)
);
