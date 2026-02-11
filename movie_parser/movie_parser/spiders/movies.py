import scrapy
from movie_parser.items import MovieParserItem


class MoviesSpider(scrapy.Spider):
    name = "movies"
    allowed_domains = ["ru.wikipedia.org"]
    start_urls = [
        "https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B_%D0%BF%D0%BE_%D0%B0%D0%BB%D1%84%D0%B0%D0%B2%D0%B8%D1%82%D1%83"
    ]

    custom_settings = {
        "FEED_EXPORT_FIELDS": ["title", "genre", "director", "year", "country", "imdb"],
        "FEEDS": {
            "movies.csv": {
                "format": "csv",
                "encoding": "utf-8",
                "overwrite": True,
            },
        },
    }

    def parse(self, response):
        movie_links = response.xpath(
            '//*[@id="mw-pages"]//div[@class="mw-category-group"]//a/@href'
        ).getall()

        for movie_link in movie_links:
            yield response.follow(movie_link, callback=self.parse_movie)

        next_page = response.xpath(
            '//a[contains(text(), "Следующая страница")]/@href'
        ).get()

        if next_page:
            next_page = response.urljoin(next_page)
            yield response.follow(next_page, callback=self.parse)

    def parse_movie(self, response):
        item = MovieParserItem()

        # Title
        item["title"] = response.xpath(
            '//*[@class="infobox-above"]/text()[normalize-space()] | '
            '//*[@class="infobox-above"]//span/text()'
        ).get()

        # Genre
        item["genre"] = response.xpath(
            '//th[contains(., "Жанр")]/following-sibling::td//text()'
        ).getall()

        # Director
        director = response.xpath(
            '//th[contains(text(), "Режиссёр")]/following-sibling::td//text()'
        ).getall()

        item["director"] = director

        # Country
        item["country"] = response.xpath(
            '//th[contains(text(), "Стран")]/following-sibling::td//text()'
        ).getall()

        # Year
        item["year"] = response.xpath(
            '//th[contains(text(), "Год") or contains(text(), "Дата") or contains(text(), "Первый показ")]/following-sibling::td//text()'
        ).getall()

        # IMDb
        item["imdb"] = response.xpath('//a[contains(@title, "imdbtitle")]/@title').get()

        yield item
