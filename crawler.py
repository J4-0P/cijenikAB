from json import loads
import datetime
import requests
import io
import polars # type: ignore
import bs4
import json
import os
import xml.etree.ElementTree as ET
import urllib.parse
import concurrent.futures
import csv
def crawlRibola(today=datetime.date.today()):
    url = "https://ribola.hr/ribola-cjenici/?date=" #29.05.2025

    date_str = today.strftime("%d.%m.%Y")
    response = requests.get(url + date_str)

    data = []
    links = []

    #we want to get the a href to get the xml download link
    soup = bs4.BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")
    if table:
        for row in table.find_all("tr"):
            tds = row.find_all("td")
            if tds:
                a_tag = tds[0].find("a")
                if a_tag and a_tag.get("href"):
                    links.append(a_tag.get("href"))
                    pass

    #if data:
       # final_df = polars.concat(data)
        #return final_df
    i = len(links)


    def process_link(link):
        result = []
        print(f"Processing link: {link}")
        if link.endswith(".xml"):
            xml_url = f"https://ribola.hr/ribola-cjenici/{link}"
            xml_response = requests.get(xml_url, headers={"User-Agent": "Mozilla/5.0"})
            text = xml_response.text
            try:
                root = ET.fromstring(text)
                prodajni_objekt = root.find("ProdajniObjekt")
                if prodajni_objekt is not None:
                    adresa = prodajni_objekt.findtext("Adresa", default="")
                    oblik = prodajni_objekt.findtext("Oblik", default="")
                    oznaka = prodajni_objekt.findtext("Oznaka", default="")
                    broj_pohrane = prodajni_objekt.findtext("BrojPohrane", default="")
                    proizvodi = prodajni_objekt.find("Proizvodi")
                    if proizvodi is not None:
                        for proizvod in proizvodi.findall("Proizvod"):
                            naziv = proizvod.findtext("NazivProizvoda", default="")
                            sifra = proizvod.findtext("SifraProizvoda", default="")
                            marka = proizvod.findtext("MarkaProizvoda", default="")
                            neto_kolicina = proizvod.findtext("NetoKolicina", default="")
                            jedinica_mjere = proizvod.findtext("JedinicaMjere", default="")
                            maloprodajna_cijena = proizvod.findtext("MaloprodajnaCijena", default="")
                            cijena_za_jedinicu_mjere = proizvod.findtext("CijenaZaJedinicuMjere", default="")
                            maloprodajna_cijena_akcija = proizvod.findtext("MaloprodajnaCijenaAkcija", default="")
                            najniza_cijena = proizvod.findtext("NajnizaCijena", default="")
                            sidrena_cijena = proizvod.findtext("SidrenaCijena", default="")
                            barkod = proizvod.findtext("Barkod", default="")
                            kategorije = proizvod.findtext("KategorijeProizvoda", default="")
                            result.append({
                                "datum": date_str,
                                "naziv": naziv,
                                "sifra": sifra,
                                "marka": marka,
                                "neto_kolicina": neto_kolicina,
                                "jedinica_mjere": jedinica_mjere,
                                "maloprodajna_cijena": maloprodajna_cijena,
                                "cijena_za_jedinicu_mjere": cijena_za_jedinicu_mjere,
                                "maloprodajna_cijena_akcija": maloprodajna_cijena_akcija,
                                "najniza_cijena": najniza_cijena,
                                "sidrena_cijena": sidrena_cijena,
                                "barkod": barkod,
                                "kategorije": kategorije,
                                "adresa": adresa,
                                "trgovina": "Ribola",
                            })
            except Exception as e:
                print(f"Error processing {link}: {e}")
        return result

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(process_link, links))

    for r in results:
        data.extend(r)
    return data


def crawlKonzum(today=datetime.date.today()):
    url = "https://www.konzum.hr/cjenici?date="
    #2025-05-29
    counter = 1
    date_str = today.strftime("%Y-%m-%d")

    response = requests.get(url + date_str+"&page=" + str(counter))
    print(f"Response status code: {response.status_code}")
    data = []
    links = []
    while response.status_code == 200:
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        for a_tag in soup.find_all("a", href=True, attrs={"format": "csv"}):
            links.append(a_tag["href"])

        if not soup.find_all("a", href=True, attrs={"format": "csv"}):
            break

        counter += 1
        print(f"Processing page {counter}...")
        response = requests.get(url + date_str + "&page=" + str(counter))

    print(f"Found {len(links)} links for Konzum")
    def process_link(link):
        result = []
        print(f"Processing link: {link}")
        link = "https://www.konzum.hr" + link if not link.startswith("https://") else link
        csv_response = requests.get(link, headers={"User-Agent": "Mozilla/5.0"})
        if csv_response.status_code == 200:
            csv_text = csv_response.text
            reader = csv.reader(io.StringIO(csv_text), delimiter=',')
            header = next(reader)
            parsed = urllib.parse.urlparse(link)
            qs = urllib.parse.parse_qs(parsed.query)
            title = qs.get("title", [""])[0]
            # Address is between first and third comma
            parts = title.split(",")
            address = ""
            if len(parts) >= 3:
                address = ",".join(parts[1:3]).strip()
            # Process each row in the CSV after the header
            for values in reader:
                # Map CSV columns by position if header is not reliable
                # 0: NAZIV PROIZVODA, 1: ŠIFRA PROIZVODA, 2: MARKA PROIZVODA, 3: NETO KOLIČINA, 4: JEDINICA MJERE, 5: MALOPRODAJNA CIJENA, 6: CIJENA ZA JEDINICU MJERE, 7: MPC ZA VRIJEME POSEBNOG OBLIKA PRODAJE, 8: NAJNIŽA CIJENA U POSLJEDNIH 30 DANA, 9: SIDRENA CIJENA NA 2.5.2025, 10: BARKOD, 11: KATEGORIJA PROIZVODA
                result.append({
                    "datum": date_str,
                    "naziv": values[0] if len(values) > 0 else "",
                    "sifra": values[1] if len(values) > 1 else "",
                    "marka": values[2] if len(values) > 2 else "",
                    "neto_kolicina": values[3] if len(values) > 3 else "",
                    "jedinica_mjere": values[4] if len(values) > 4 else "",
                    "maloprodajna_cijena": values[5] if len(values) > 5 else "",
                    "cijena_za_jedinicu_mjere": values[6] if len(values) > 6 else "",
                    "maloprodajna_cijena_akcija": values[7] if len(values) > 7 else "",
                    "najniza_cijena": values[8] if len(values) > 8 else "",
                    "sidrena_cijena": values[9] if len(values) > 9 else "",
                    "barkod": values[10] if len(values) > 10 else "",
                    "kategorije": values[11] if len(values) > 11 else "",
                    "adresa": address,
                    "trgovina": "Konzum"
                })

        print(f"Processed {len(result)} entries from {link}")
        return result
    # Dynamically set max_workers based on your CPU (6 cores, 12 threads)
    max_workers = min(32, (os.cpu_count() or 1) * 5)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_link, links))
    for r in results:
        data.extend(r)
    return data
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, date
import time
import requests
import csv
import io
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
def crawlSpar(today=date.today()):
    data = []
    url = "https://www.spar.hr/datoteke_cjenici/index.html"
    options = Options()
    # options.add_argument("--headless")  # comment out for visual debug
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)

    driver.get(url)
    wait.until(EC.presence_of_element_located((By.ID, "datePicker")))

    date_str = today.strftime('%Y-%m-%d')
    print(date_str)

    # Find date picker fresh
    date_picker = driver.find_element(By.ID, "datePicker")
    date_picker.click()
    driver.execute_script(
        "arguments[0].value = arguments[1];"
        "arguments[0].dispatchEvent(new Event('input', { bubbles: true }));"
        "arguments[0].dispatchEvent(new Event('change', { bubbles: true }));",
        date_picker, date_str
    )
    date_picker.send_keys("\t")

    # Wait for the DOM to reload links
    wait.until(EC.presence_of_element_located((By.LINK_TEXT, "Preuzmi")))

    # Re-find the links **after** the DOM update
    links_elements = driver.find_elements(By.LINK_TEXT, "Preuzmi")

    # Extract hrefs
    links = [a.get_attribute('href') for a in links_elements]
    def process_link(link):
            result = []
            print(f"Processing link: {link}")
            if not link.startswith("https://"):
                link = "https://www.spar.hr/datoteke_cjenici/" + link
            
            session = requests.Session()
            retry_strategy = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            
            try:
                response = session.get(link, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
                response.raise_for_status()
            
                
                csv_text = response.text
                reader = csv.reader(io.StringIO(csv_text), delimiter=';')  # delimiter is ';' not ','
                header = next(reader)
                parsed = urllib.parse.urlparse(link)
                qs = urllib.parse.parse_qs(parsed.query)
                title = qs.get("title", [""])[0]

                parts = title.split(",")
                address = ""
                if len(parts) >= 3:
                    address = ",".join(parts[1:3]).strip()

                for values in reader:
                    result.append({
                        "datum": date_str,
                        "naziv": values[0] if len(values) > 0 else "",
                        "sifra": values[1] if len(values) > 1 else "",
                        "marka": values[2] if len(values) > 2 else "",
                        "neto_kolicina": values[3] if len(values) > 3 else "",
                        "jedinica_mjere": values[4] if len(values) > 4 else "",
                        "maloprodajna_cijena": values[5] if len(values) > 5 else "",
                        "cijena_za_jedinicu_mjere": values[6] if len(values) > 6 else "",
                        "maloprodajna_cijena_akcija": values[7] if len(values) > 7 else "",
                        "najniza_cijena": values[8] if len(values) > 8 else "",
                        "sidrena_cijena": values[9] if len(values) > 9 else "",
                        "barkod": values[10] if len(values) > 10 else "",
                        "kategorije": values[11] if len(values) > 11 else "",
                        "adresa": address,
                        "trgovina": "Spar"
                    })
                print(f"Processed {len(result)} entries from {link}")
                return result
            except requests.RequestException as e:
                print(f"Failed to get {link}: {e}")

    driver.quit()
    max_workers = min(32, (os.cpu_count() or 1) * 5)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_link, links))
    for r in results:
        data.extend(r)
    return data
    #naziv;šifra;marka;neto količina;jedinica mjere;MPC (EUR);cijena za jedinicu mjere (EUR);MPC za vrijeme posebnog oblika prodaje (EUR);Najniža cijena u posljednjih 30 dana (EUR);sidrena cijena na 2.5.2025. (EUR);barkod;kategorija proizvoda

# import zipfile
# import re

#    return all_data
#if __name__ == "__main__":
    # todays = crawlSpar(today=datetime.strptime("29.5.2025", "%d.%m.%Y").date())
    # yesterdays= crawlSpar()
    # print(todays == yesterdays)
#     today = datetime.today()
#    # konzum_data = CrawlPlodine()
#     name = "output/eurospin_" + today.strftime("%Y-%m-%d") + ".ndjson"
#     with open(name, "w", encoding="utf-8") as f:
#         for item in konzum_data:
#             f.write(json.dumps(item, ensure_ascii=False) + "\n")
if __name__ == "__main__":
    today = date.today()
    konzum = crawlKonzum()
    spar = crawlSpar()
    ribola = crawlRibola()
    combined = [*konzum, *spar, *ribola]
    name = "output/combined_" + today.strftime("%Y-%m-%d") + ".ndjson"
    with open(name, "w", encoding="utf-8") as f:
        for item in combined:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")





