-- create clean scraping result
CREATE TABLE IF NOT EXISTS kompas_scraping_result (
    tanggal date default current_timestamp,
    scrape_time VARCHAR NOT NULL,
    story_source VARCHAR NOT NULL,
    story_news_tag VARCHAR NOT NULL,
    story_release_date VARCHAR NOT NULL,    
    story_title VARCHAR NOT NULL,
    story_url VARCHAR NOT NULL,
    most_common_words VARCHAR NOT NULL);


