import requests
from bs4 import BeautifulSoup
import csv
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor

class RequestScheduler:

    def __init__(self):
        self.base_delay = 3
        self.max_delay = 30
        self.current_delay = 3

    def wait(self):
        sleep_time = random.uniform(self.current_delay, self.current_delay + 2)
        print(f"Scheduler sleeping {round(sleep_time,2)} seconds")
        time.sleep(sleep_time)

    def success(self):
        # Slowly speed up
        self.current_delay = max(self.base_delay, self.current_delay * 0.9)

    def rate_limited(self):
        # Slow down aggressively
        self.current_delay = min(self.max_delay, self.current_delay * 1.7)
        print(f"Rate limit detected. Increasing delay to {round(self.current_delay,2)} seconds")

BASE_URL = "https://www.thomann.se/cat_BF_{}.html?ls=1000&pg={}"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
]

HEADERS = {
    "User-Agent": random.choice(USER_AGENTS),
    "Accept-Language": "en-US,en;q=0.9"
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)
scheduler = RequestScheduler()


def fetch_page(url, retries=5):

    for attempt in range(retries):

        scheduler.wait()

        try:
            response = SESSION.get(
                url,
                headers={"User-Agent": random.choice(USER_AGENTS)},
                timeout=10
            )

            # --- 404: brand/page does not exist ---
            if response.status_code == 404:
                print(f"Page not found (404): {url}")
                return None

            # --- 429: rate limit ---
            if response.status_code == 429:
                scheduler.rate_limited()
                continue

            # --- other server errors ---
            if response.status_code >= 500:
                print("Server error. Retrying...")
                scheduler.rate_limited()
                continue

            response.raise_for_status()

            scheduler.success()

            return response.text

        except requests.RequestException as e:

            wait = random.uniform(5, 15)
            print(f"Request failed: {e}")
            print(f"Retrying in {round(wait,1)} seconds...")
            time.sleep(wait)

    return None

def discover_brands():
    """Automatically discover Thomann brand slugs."""

    url = "https://www.thomann.se/cat_brands.html"

    print("Discovering brands...")

    html = fetch_page(url)

    if not html:
        print("Failed to load brand directory.")
        return []

    soup = BeautifulSoup(html, "html.parser")

    brands = set()

    for link in soup.find_all("a", href=True):

        href = link["href"]

        if "cat_BF_" in href and ".html" in href:

            slug = href.split("cat_BF_")[1].split(".html")[0]

            brands.add(slug)

    brands = sorted(brands)

    print(f"Discovered {len(brands)} brands")

    return brands

def save_debug_html(filename, html):

    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)

def extract_items_json(html):

    start = html.find('"items":[')

    if start == -1:
        return None

    start = html.find('[', start)

    bracket_count = 0

    for i in range(start, len(html)):

        if html[i] == '[':
            bracket_count += 1

        elif html[i] == ']':
            bracket_count -= 1

            if bracket_count == 0:
                return html[start:i+1]

    return None

def parse_products(html):

    items_json = extract_items_json(html)

    if not items_json:
        print("Could not find items array")
        return []

    items = json.loads(items_json)

    products = []

    for item in items:

        product = {
            "sku": item.get("item_id"),
            "name": item.get("item_name"),
            "brand": item.get("item_brand"),
            "price": item.get("price"),
            "currency": item.get("currency"),
            "category": item.get("item_category"),
            "url": f"https://www.thomann.de/se/{item.get('item_id')}.htm"
        }

        products.append(product)

    return products

def scrape_all_pages(brand, global_skus):
    """Scrape catalog until no more products appear."""

    page = 1
    all_products = []

    while True:

        url = BASE_URL.format(brand, page)
        print(f"Scraping brand {brand} | page {page}")

        html = fetch_page(url)

        if html is None:

            if page == 1:
                print(f"Skipping brand '{brand}' (no valid catalog page)")
                return []

            break

        print(f"Downloaded {len(html)} characters")

        save_debug_html(f"{brand}_page_{page}.html", html)

        if not html:
            break

        products = parse_products(html)

        if not products:
            print("No products found. Stopping.")
            break

        new_products = []

        for p in products:
            sku = p.get("sku")

            if sku and sku not in global_skus:
                global_skus.add(sku)
                new_products.append(p)

        print(f"Found {len(new_products)} new products")

        all_products.extend(new_products)

        page += 1



    return all_products


def save_to_csv(products):
    """Save results to CSV."""

    if not products:
        print("No products to save")
        return

    keys = products[0].keys()

    with open("thomann list.csv", "w", newline="", encoding="utf-8") as f:

        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(products)

    print(f"Saved {len(products)} products to thomann list.csv")


def print_summary(products):

    print("\n--- SCRAPE SUMMARY ---")
    print("Total products:", len(products))

    prices = [float(p["price"]) for p in products if p["price"]]

    if prices:
        print("Min price:", min(prices))
        print("Max price:", max(prices))
        print("Average price:", round(sum(prices) / len(prices), 2))


def main():

    brands = discover_brands()

    if not brands:
        print("No brands found. Exiting.")
        return

    all_products = []
    global_skus = set()

    for brand in brands:

        print(f"\n====== SCRAPING {brand.upper()} ======")

        products = scrape_all_pages(brand, global_skus)

        all_products.extend(products)

    print_summary(all_products)

    save_to_csv(all_products)


if __name__ == "__main__":
    main()

