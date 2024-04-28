# EurostatBot

EurostatBot is a Wikipedia bot to synchronize statistics in Wikipedia with
those published by [Eurostat](https://en.wikipedia.org/wiki/Eurostat),
Europeean Statistical Office.

EurostatBot is written in [Python](https://www.python.org/).
It uses [SDMX](https://sdmx.org/)(Statistical Data and Metadata eXchange)
standard to query Eurostat.
It uses [pandaSDMX](https://pandasdmx.readthedocs.io/) package to execute
queries and [pywikibot](https://doc.wikimedia.org/pywikibot/stable/) package
to update Wikipedia.
