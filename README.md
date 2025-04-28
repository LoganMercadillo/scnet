# Submarine Cable Network (SCNET)

Retrieves, parses, cleans, and stores data about the submarine cable network.

Collects data from:

- https://www.submarinecablemap.com


## Instructions

Navigate to the project root.

Create and activate a virtual environment:

```
pyenv virtualenv 3.10.16 scnet
pyenv activate scnet
```

or use venv using using 3.10.16:

```
python3 -m venv venv
source venv/bin/activate
```


Install requirements:

```
python3 -m pip install --require-virtualenv -r requirements.txt
```

To run the scraper by itself, execute the following (from the project root!):

```
python3 update/scrapers/scm_scraper.py
```

