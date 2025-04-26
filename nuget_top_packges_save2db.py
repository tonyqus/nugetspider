from bs4 import BeautifulSoup
import requests
import time
import json
import pandas as pd
import psycopg2
import locale

class NuGetCrawler:
    BASE_URL = "https://www.nuget.org"

    def __init__(self):
        self.session = requests.Session()

    def search_packages(self, query, page=1,ignore_MSFT_packages=False):
        """Scrape search results from nuget.org."""
        search_url = f"{self.BASE_URL}/packages"
        params = {"q": query, "includeComputedFrameworks": False,"prerel":False,"sortby":"totalDownloads-desc", "page": page}
        response = self.session.get(search_url, params=params)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        packages = []

        for package in soup.select(".package"):
            name = package.select_one(".package-title a").text.strip()
            version = package.select_one(".package-title a")["data-package-version"].strip()
            description = package.select_one(".package-details").text.strip()
            downloads = package.select_one(".package-list .ms-Icon--Download").find_parent().text.strip().replace(" total downloads", "")
            if(ignore_MSFT_packages == True):
                if "Microsoft." in name or "System." in name or "Azure." in name or "Xamarin." in name or "NuGet." in name:
                    continue
            packages.append({"name": name, "version": version, "description": description,"downloads":downloads, "ranking": 0})

        return packages

    def crawl_packages(self, query, max_packages=50,ignore_MSFT_packages=False):
        """Crawl packages by scraping nuget.org."""
        crawled_data = []
        page = 1
        rank = 0

        while len(crawled_data) < max_packages:
            
            packages = self.search_packages(query, page=page,ignore_MSFT_packages=ignore_MSFT_packages)
            if not packages:
                break

            for package in packages:
                rank += 1
                package["ranking"] = rank

            crawled_data.extend(packages)
            print(f"Page {page} crawled, found {len(packages)} packages.")
            if len(crawled_data) >= max_packages:
                break

            page += 1
            time.sleep(1)  # Be polite and avoid overwhelming the server

        return crawled_data

def save_to_postgresql(packages, db_config):
    """Save the package data to a PostgreSQL table."""
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()

    # Create table if it doesn't exist
#    cursor.execute("""
#        CREATE TABLE IF NOT EXISTS nuget_packages (
#            id SERIAL PRIMARY KEY,
#            package TEXT NOT NULL,
#            version TEXT NOT NULL,
#            downloads BIGINT NOT NULL
#        )
#    """)

    # Truncate the table before inserting new data
    cursor.execute("""
        TRUNCATE TABLE nuget_packages
    """)
    # Insert package data
    for package in packages:
        isMSFTPackage = False
        name = package['name']
        if "Microsoft." in name or "System." in name or "Azure." in name or "Xamarin." in name or "NuGet." in name:
            isMSFTPackage= True
        cursor.execute("""
            INSERT INTO nuget_packages ("PackageName", "Downloads", "IsMSFTPackage","LatestVersion")
            VALUES (%s, %s, %s::int::bit, %s)
        """, (package['name'], locale.atoi(package['downloads']), isMSFTPackage, package['version']))

    connection.commit()
    cursor.close()
    connection.close()

if __name__ == "__main__":

    locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' ) 

    crawler = NuGetCrawler()
    top_packages = crawler.crawl_packages("", max_packages=100, ignore_MSFT_packages=False)

    db_config = {
        "dbname": "tonyqus",
        "user": "postgres",
        "host": "localhost",
        "port": 5432
    }

    # Save the packages to PostgreSQL
    save_to_postgresql(top_packages, db_config)
    print("Packages saved to PostgreSQL.")