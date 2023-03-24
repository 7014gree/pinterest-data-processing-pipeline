-- Original implementation had unique_id as the primary key.
-- Since the emulation would repeat itself after a time, this led to many errors using "append".
-- Using "overwrite" would delete the table every time a new batch was processed, so could not be used.
-- Using "ignore" just wasn't working for me at all.
/*
CREATE TABLE experimental_data (
	"index" INT,
	category VARCHAR(30),
	unique_id UUID UNIQUE PRIMARY KEY,
	title VARCHAR(200),
	description VARCHAR(200),
	poster_name VARCHAR(50),
	follower_count INT,
	tag_list VARCHAR(500),
	is_image_or_video VARCHAR(5),
	image_src VARCHAR(200),
	downloaded BOOL,
	save_location VARCHAR(40));
*/

-- Now using id SERIAL as a PRIMARY KEY.
-- Can perform SQL operations to remove duplicates when the data is being retrieved.
CREATE TABLE experimental_data (
	id SERIAL PRIMARY KEY,
	"index" INT,
	category VARCHAR(30),
	unique_id UUID,
	title VARCHAR(200),
	description VARCHAR(200),
	poster_name VARCHAR(50),
	follower_count INT,
	tag_list VARCHAR(500),
	is_image_or_video VARCHAR(5),
	image_src VARCHAR(200),
	downloaded BOOL,
	save_location VARCHAR(40));

SELECT * FROM experimental_data WHERE unique_id = '9bf39437-42a6-4f02-99a0-9a0383d8cd70';
	
-- DELETE FROM experimental_data