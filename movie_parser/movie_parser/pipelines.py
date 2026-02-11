# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import re


class MovieParserPipeline:
    STOP_WORDS = {",", "[", "]", "(", ")", "/", "и", "[вд]"}

    def _clean_data(self, data, to_lower=False):
        cleaned = [x.strip() for x in data if x.strip()]
        cleaned = [x for x in cleaned if not x.isdigit()]
        if to_lower:
            cleaned = [x.lower() for x in cleaned]
        cleaned = [x for x in cleaned if x not in self.STOP_WORDS]
        cleaned = [x for x in cleaned if ".mw" not in x]
        return cleaned

    def _finalize_string(self, text):
        if not text:
            return None
        return (
            text.replace("\xa0", " ").replace(",", "").replace("  ", " ").strip()
            or None
        )

    def process_item(self, item, spider):
        # Title
        title = item.get("title")
        if title:
            item["title"] = self._finalize_string(title)

        # Genre
        genre = item.get("genre")
        if genre:
            cleaned_genres = self._clean_data(genre, to_lower=True)
            merged_genres = []
            i = 0
            while i < len(cleaned_genres):
                current = cleaned_genres[i]
                if current == "-" and merged_genres and i + 1 < len(cleaned_genres):
                    merged_genres[-1] += f"-{cleaned_genres[i + 1]}"
                    i += 2
                else:
                    merged_genres.append(current)
                    i += 1
            if merged_genres:
                full_genre_str = " ".join(merged_genres)
                words = full_genre_str.split()
                selected_words = []
                if words:
                    if len(words) > 1 and words[0] == "фильм" and words[1] == "ужасов":
                        selected_words.append(words[0])
                        selected_words.append(words[1])
                    else:
                        selected_words.append(words[0])
                        if words[0].endswith(("ая", "ое")):
                            if len(words) > 1:
                                selected_words.append(words[1])
                                if words[1].endswith(("ая", "ое")):
                                    if len(words) > 2:
                                        selected_words.append(words[2])
                genre_str = " ".join(selected_words)
                item["genre"] = self._finalize_string(genre_str)
            else:
                item["genre"] = None
        else:
            item["genre"] = None

        # Director
        director = item.get("director")
        if director:
            directors = self._clean_data(director)
            if directors:
                director_str = directors[0]
                if "," in director_str:
                    director_str = director_str.split(",")[0]
                item["director"] = self._finalize_string(director_str)
            else:
                item["director"] = None
        else:
            item["director"] = None

        # Country
        country = item.get("country")
        if country:
            countries = self._clean_data(country)
            item["country"] = self._finalize_string(countries[0]) if countries else None
        else:
            item["country"] = None

        # Year
        year = item.get("year")
        valid_years = []
        if year:
            for y in year:
                valid_years.extend(re.findall(r"\d{4}", y))
        item["year"] = valid_years[0] if valid_years else None

        # IMDb
        imdb = item.get("imdb")
        if imdb:
            id = imdb.split(":")[-1].strip()
            item["imdb"] = id if id else None

        return item
