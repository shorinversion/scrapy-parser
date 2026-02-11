import scrapy
import re


class MoviesSpider(scrapy.Spider):
    name = "movies"
    allowed_domains = ["ru.wikipedia.org"]
    start_urls = [
        "https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B_%D0%BF%D0%BE_%D0%B0%D0%BB%D1%84%D0%B0%D0%B2%D0%B8%D1%82%D1%83"
    ]

    def parse(self, response):
        movie_links = response.xpath(
            '//*[@id="mw-pages"]//div[@class="mw-category-group"]//a/@href'
        ).getall()

        count = 0
        for movie_link in movie_links:
            yield response.follow(movie_link, callback=self.parse_movie)
            count += 1
            if count == 50:
                break

        # test_link = ""

        # yield response.follow(test_link, callback=self.parse_movie)

    def parse_movie(self, response):
        title = ""
        genre = ""
        director = ""
        country = ""
        year = ""
        imdb = ""

        title = (
            response.xpath('//*[@class="infobox-above"]/text()')
            .get()
            .replace("\xa0", " ")
        )

        genres = response.xpath(
            '//th[contains(., "Жанр")]/following-sibling::td//text()'
        ).getall()

        if genres:

            genres = [i.strip() for i in genres if i.strip()]
            genres = [
                i for i in genres if i and i not in [",", "[", "]", "(", ")", "/"]
            ]
            genres = [i for i in genres if not i.isdigit()]
            genres = [i.lower() for i in genres]
            genres = [i for i in genres if i != "и" and i != "[вд]"]

            processed_genres = []
            i = 0
            while i < len(genres):
                if genres[i] == "-":
                    left = processed_genres.pop()
                    right = genres[i + 1]
                    processed_genres.append(f"{left}-{right}")
                    i += 2
                else:
                    processed_genres.append(genres[i])
                    i += 1

            if (
                processed_genres[0].endswith("ая")
                or processed_genres[0].endswith("ий")
                or processed_genres[0].endswith("ое")
            ):
                processed_genres = " ".join(processed_genres)
            else:
                processed_genres = processed_genres[0]

            genre = processed_genres

            if "," in genre:
                genre = genre.split(",")[0]

            genre = " ".join(genre.split(" ")[0:2])

        director = response.xpath(
            '//th[contains(text(), "Режиссёр")]/following-sibling::td//a/text()'
        ).get()

        if director is None:
            director = response.xpath(
                '//th[contains(text(), "Режиссёр")]/following-sibling::td//span/text()'
            ).get()

        countries = response.xpath(
            '//th[contains(text(), "Стран")]/following-sibling::td//text()'
        ).getall()

        if countries:
            countries = [i.strip() for i in countries if i.strip()]
            countries = [
                i for i in countries if i and i not in [",", "[", "]", "(", ")"]
            ]
            countries = [i for i in countries if not i.isdigit()]
            if countries:
                country = countries[0]

        years = response.xpath(
            '//th[contains(text(), "Год") or contains(text(), "Дата")]/following-sibling::td//text()'
        ).getall()

        if years:
            valid_years = []
            for y in years:
                years_found = re.findall(r"\d{4}", y)
                valid_years.extend(years_found)

            if valid_years:
                year = valid_years[0]

        imdb = response.xpath('//a[contains(@title, "imdbtitle")]/@title').get()

        if imdb:
            imdb = imdb.split(":")[-1]

        yield {
            "url": response.url,
            "title": title,
            "genre": genre,
            "director": director,
            "country": country,
            "year": year,
            "imdb": imdb,
        }
