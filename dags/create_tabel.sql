-- create raw scraping result
CREATE TABLE IF NOT EXISTS raw_scraping_result (
    tanggal DATE NOT NULL,
    response VARCHAR NOT NULL);

-- create clean scraping result
CREATE TABLE IF NOT EXISTS clean_scraping_result (
    id SERIAL,
    response VARCHAR NOT NULL,
    story_source VARCHAR NOT NULL,
    story_news_tag VARCHAR NOT NULL,
    story_release_date DATE NOT NULL,
    story_title VARCHAR NOT NULL,
    story_url DATE NOT NULL,
    most_common_words VARCHAR NOT NULL);


