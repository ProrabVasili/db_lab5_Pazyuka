INSERT INTO cuisine (type_cuisine)
VALUES
('Bakery'),
('North Indian'),
('Fast Food'),
('Desserts'),
('Pizza');

INSERT INTO location (city)
VALUES
('Agra'),
('Ranchi'),
('Mumbai'),
('Salem'),
('Kanpur');

INSERT INTO restaurant (restaurant_name, rating, city_id)
VALUES
('Campus Bakers', 4.3, 1),
('Grameen Kulfi', 4.5, 1),
('OMG Cafe', 4.2, 2),
('Brijwasi Pure Veg', 4.2, 2),
('Le 15 Patisserie', 4.6, 3),
('Cake Factory', 3.0, 3),
('Snackos Resto Cafe', 3.6, 4),
('ChappatiKings', 4.2, 4),
('Shree Desi Vyanjan', 3.8, 5),
('Cake My Day', 4.6, 5);

INSERT INTO restaurant_cuisine (restaurant_id, cuisine_id)
VALUES
(1, 1),
(1, 3),
(1, 5),
(2, 4),
(3, 2),
(3, 4),
(4, 2),
(4, 3),
(5, 1),
(5, 4),
(6, 1),
(6, 4),
(7, 3),
(7, 5),
(8, 2),
(9, 2),
(9, 5),
(10, 1),
(10, 4);








