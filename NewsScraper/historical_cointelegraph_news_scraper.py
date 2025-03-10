import os
from dotenv import load_dotenv
load_dotenv('../.env')
from datetime import datetime, timedelta

def NewsMQSetup():
    from kafka.admin import NewTopic
    from kafka import KafkaAdminClient
    from kafka import KafkaProducer

    kafka_client = KafkaAdminClient(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    if os.getenv("NEWS_MQ") not in kafka_client.list_topics():
        kafka_client.create_topics([NewTopic(os.getenv("NEWS_MQ"), num_partitions=3, replication_factor=1)])

    producer = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"), batch_size=16384, linger_ms=1000, )
    return producer

def process_article(article, kafka_article_data, domain='https://cointelegraph.com'):
    import uuid

    kafka_article_data['news_id'] = str(uuid.uuid4())
    news_url = article.find('a', attrs={'class':'post-card-inline__title-link'})
    if news_url and news_url.has_attr('href'):
        kafka_article_data['news_url'] = domain + news_url.get('href')

    if news_url:
        kafka_article_data['title'] = news_url.get_text().strip()

    publish_time = article.find('time')
    if publish_time and publish_time.has_attr("datetime"):
        kafka_article_data['publish_time'] = datetime.strptime(publish_time.get('datetime'), "%Y-%m-%d")

    author = article.find('p', attrs={'class':'post-card-inline__author'}).find('a')
    if author and author.has_attr('href'):
        kafka_article_data['author_url'] = domain + author.get('href')
    if author:
        kafka_article_data['author_name'] = author.get_text().strip()

    source_article_description = article.find('p', attrs={'class':'post-card-inline__text'})
    if source_article_description:
        kafka_article_data['source_article_description'] = source_article_description.get_text()
    reach = article.find('div', attrs={"class":"post-card-inline__stats"})
    if reach:
        kafka_article_data['reach'] = int(reach.get_text().strip())

    return kafka_article_data

async def main():

    from selenium import webdriver
    from selenium.webdriver import ActionChains
    from datetime import datetime, timedelta
    from bs4 import BeautifulSoup

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    producer = NewsMQSetup()
    
    from pymongo import AsyncMongoClient
    mongo_client = AsyncMongoClient("localhost", 27017)

    def post_scrape_message(message):
        nonlocal producer
        import json

        producer.send(os.getenv("NEWS_MQ"), json.dumps(message).encode('utf-8'), key=message['news_id'].encode('utf-8'))
        return
    
    async def flush_data_to_db(processed_articles):
        nonlocal mongo_client
        db = mongo_client[os.getenv("MONGO_DATABASE")]
        if os.getenv("MONGO_NEWS_COLLECTION") not in await db.list_collection_names():
            collection = db.create_collection(os.getenv("MONGO_NEWS_COLLECTION"))

        collection = db.get_collection(os.getenv("MONGO_NEWS_COLLECTION"))

        batch = []
        for article in processed_articles:
            batch.append(article)
            if len(batch) == 10:
                await collection.insert_many(batch)
                batch = []
        if batch:
            await collection.insert_many(batch)

    domain = "https://cointelegraph.com"

    tags = ['markets', 'technology','business','regulation', 'investments', 'nft']
    
    news_collection = mongo_client[os.getenv("MONGO_DATABASE")].get_collection(os.getenv("MONGO_NEWS_COLLECTION"))
    
    try:
        last_news_timestamp = await news_collection.find().sort({'news_collection_time':-1}).limit(1).next()
    except StopAsyncIteration:
        last_news_timestamp = None
        print("Collection was empty. Will try to collect last 2 years data")
    
    latest_news_url = "" if not last_news_timestamp else last_news_timestamp['news_url']
    last_news_timestamp = last_news_timestamp.get("news_collection_time") if last_news_timestamp else None
    
    if not last_news_timestamp:
        last_news_timestamp = datetime.today() - timedelta(days=365*2)
        
    for tag in tags:
        driver.get(domain + "/tags/" + tag)
        print("Starting with", domain + "/tags/" + tag)
        
        total_tag_articles = 0
        processed_articles = []
        while total_tag_articles < 5000:
            processed_articles = []
            elems = driver.find_elements('xpath', '(//article[@class="post-card-inline"])[last()]')
            if not elems:
                break
            
            news_timestamp = elems[0].find_elements('xpath', './div/div/div/time')[0].get_attribute('datetime')
            news_timestamp = datetime.strptime(news_timestamp, "%Y-%m-%d")

            if news_timestamp < last_news_timestamp:
                break

            soup = BeautifulSoup(driver.page_source)
            articles = soup.find_all('article', attrs={'class' : 'post-card-inline'})[total_tag_articles:]
            
            for article in articles:
                article_json_data = {}
                article_json_data['news_source'] = domain
                article_json_data['page_path'] = domain + '/tags/' + tag
                article_json_data['news_collection_time'] = datetime.today()
                article_json_data = process_article(article, article_json_data, domain=domain)
                if article_json_data['news_url']:
                    break
                post_scrape_message({'news_id' : article_json_data['news_id'], 'news_url' : article_json_data['news_url']})
                processed_articles.append(article_json_data)
            
            await flush_data_to_db(processed_articles)

            ActionChains(driver).scroll_to_element(elems[0]).perform()
            driver.implicitly_wait(1)
            total_tag_articles += len(articles)
            print(total_tag_articles)
        
        producer.flush()
        if processed_articles:
            await flush_data_to_db(processed_articles)

    driver.close()
    await mongo_client.aclose()

if __name__ == "__main__":
    import asyncio

    asyncio.run(main())