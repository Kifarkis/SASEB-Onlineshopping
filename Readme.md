# SAS EuroBonus Shopping Tracker

Tracks offers and campaigns on SAS EuroBonus Shopping
(onlineshopping.flysas.com) and highlights new ones as they appear.

## How it works

A scheduled GitHub Action runs every few hours, fetches the current
offers, diffs against the previous snapshot, and publishes a static
HTML page via GitHub Pages.

## Status

🚧 Under construction.

## Stack

- Python (scraper)
- GitHub Actions (scheduler)
- GitHub Pages (hosting)

