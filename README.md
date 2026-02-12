# Wiki Movie Info Parser

### Environment

```bash
py -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

### Run for test

```bash
cd movie_parser
scrapy crawl movies -s CLOSESPIDER_ITEMCOUNT=500
```
