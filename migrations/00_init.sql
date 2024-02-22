DROP TABLE IF EXISTS category CASCADE;
CREATE TABLE category (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL
);

DROP TABLE IF EXISTS product CASCADE;
CREATE TABLE product (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  category_id INT NOT NULL,
  hits BIGINT NOT NULL,
  FOREIGN KEY (category_id) REFERENCES category(id)
);
CREATE INDEX ON product USING BTREE (category_id, hits);

DROP TABLE IF EXISTS tag CASCADE;
CREATE TABLE tag (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL
);

DROP TABLE IF EXISTS product_tag CASCADE;
CREATE TABLE product_tag (
  tag_id INT,
  product_id INT,
  FOREIGN KEY (product_id) REFERENCES product(id),
  FOREIGN KEY (tag_id) REFERENCES tag(id),
  PRIMARY KEY (tag_id, product_id)
);

-- Generate 100 categories
INSERT INTO category (name)
SELECT 'Category ' || generate_series
FROM generate_series(1, 100);

-- Generate 500 tags
INSERT INTO tag (name)
SELECT 'Tag ' || generate_series
FROM generate_series(1, 500);

-- Generate one million products and assign them to a category and random tags
INSERT INTO product (name, description, category_id, hits)
SELECT 'Product ' || generate_series,
       'Description for Product ' || generate_series,
       floor(random() * 100) + 1,
       floor(random() * 1000)
FROM generate_series(1, 1000000);

-- -- Assign random tags to the products
-- INSERT INTO product_tag (product_id, tag_id)
-- SELECT generate_series % 1000000 + 1, 
--        floor(random() * 500) + 1
-- FROM generate_series(1, 5000000)
-- ON CONFLICT DO NOTHING;

