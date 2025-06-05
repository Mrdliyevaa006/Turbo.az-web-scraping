import requests
from bs4 import BeautifulSoup
import csv
import time
from concurrent.futures import ThreadPoolExecutor

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})

def get_ids(page_count=1):
    base_url = "https://turbo.az/autos?page={}"
    all_ids = set()

    for page in range(1, page_count + 1):
        print(f"Səhifə {page} işlənir...")
        url = base_url.format(page)
        try:
            response = session.get(url)
            if response.status_code != 200:
                print(f"Səhifə {page} yüklənmədi.")
                continue
        except Exception as e:
            print(f"Xəta baş verdi: {e}")
            continue

        soup = BeautifulSoup(response.text, "lxml")
        links = soup.find_all("a", href=True)

        for link in links:
            href = link["href"]
            if href.startswith("/autos/"):
                try:
                    car_id = href.split("/autos/")[1].split("-")[0]
                    if car_id.isdigit():
                        all_ids.add(car_id)
                except IndexError:
                    continue  

        time.sleep(0.3)  

    print(f"Toplam {len(all_ids)} elan tapıldı.")
    return list(all_ids)


def fetch_with_retry(url, retries=3):
    for i in range(retries):
        try:
            response = session.get(url)
            if response.status_code == 200:
                return response
        except:
            time.sleep(1 + i)
    return None


def get_car_info(car_id):
    url = f"https://turbo.az/autos/{car_id}"
    response = fetch_with_retry(url)
    if not response:
        print(f"{car_id} üçün məlumat tapılmadı.")
        return None

    soup = BeautifulSoup(response.text, "lxml")

    try:
        main_info = soup.select_one('.product-title').get_text(strip=True)
    except AttributeError:
        main_info = ""

    extra_info_elements = soup.find_all(class_="product-section--wide")
    extra_info = ' - '.join([e.get_text(strip=True) for e in extra_info_elements])

    try:
        description = soup.find(class_="product-description__content").get_text(strip=True)
    except AttributeError:
        description = ""

    properties_elements = soup.find_all(class_='product-extras')
    properties = ' - '.join([p.get_text(strip=True) for p in properties_elements])

    return [main_info, extra_info, description, properties]


def scrape_and_save(page_count=1):
    car_ids = get_ids(page_count)
    
    print("Elan detalları yığılır...")

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(get_car_info, car_ids))

    clean_data = [row for row in results if row]

    with open("car_data.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(['Main_Info', 'Extra_Info', 'Description', 'Properties'])
        writer.writerows(clean_data)

    print(f"{len(clean_data)} elan CSV-yə yazıldı ✅")

scrape_and_save(page_count=500)