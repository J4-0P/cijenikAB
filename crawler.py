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
import libarchive
import time
from datetime import datetime, date
def crawlRibola(dateToCrawl=date.today()):
    url = "https://ribola.hr/ribola-cjenici/?date=" #29.05.2025

    date_str = dateToCrawl.strftime("%d.%m.%Y")
    dateactual = dateToCrawl.strftime("%d.%m.%Y")
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
        print("spavam na",link)
        time.sleep(0.2)
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
                                "datum": dateactual,
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


def crawlKonzum(dateToCrawl=date.today()):
    url = "https://www.konzum.hr/cjenici?date="
    #2025-05-29
    counter = 1
    date_str = dateToCrawl.strftime("%Y-%m-%d")
    dateactual = dateToCrawl.strftime("%d.%m.%Y")
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
                # Decode each value from latin1 and encode to utf-8, then decode back to str
                values = [v.encode('latin1').decode('utf-8') if isinstance(v, str) else v for v in values]
                # Map CSV columns by position if header is not reliable
                # 0: NAZIV PROIZVODA, 1: ŠIFRA PROIZVODA, 2: MARKA PROIZVODA, 3: NETO KOLIČINA, 4: JEDINICA MJERE, 5: MALOPRODAJNA CIJENA, 6: CIJENA ZA JEDINICU MJERE, 7: MPC ZA VRIJEME POSEBNOG OBLIKA PRODAJE, 8: NAJNIŽA CIJENA U POSLJEDNIH 30 DANA, 9: SIDRENA CIJENA NA 2.5.2025, 10: BARKOD, 11: KATEGORIJA PROIZVODA
                result.append({
                    "datum": dateactual,
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
        elif csv_response.status_code == 403:
            print("Konzum te spoofao, sorry; malo manje pokušaja kasnije")
            return []
        else:
            print("Greška kod Konzuma, status",csv_response.status_code)
            return []
        print(f"Processed {len(result)} entries from {link}")
        return result
    # Dynamically set max_workers based on your CPU (6 cores, 12 threads)
    max_workers = min(32, (os.cpu_count() or 1) * 5)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_link, links))
    for r in results:
        data.extend(r)
    return data

import time
import requests
import csv
import io
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
def crawlSpar(dateToCrawl=date.today()):
    data = []
    date_string = dateToCrawl.strftime('%Y%m%d')
    url = f'https://www.spar.hr/datoteke_cjenici/Cjenik{date_string}.json'
    dateactual = dateToCrawl.strftime("%d.%m.%Y")
    try:
        res = requests.get(url)
        res.raise_for_status()
        dataaa = res.json()
        links = [file['URL'] for file in dataaa.get('files', [])]
        titles = [file['name'] for file in dataaa.get('files', [])]
        dictoflinks = dict(zip(titles, links))
    except Exception as e:
        print("Spar neuspješan",e)
    # Extract hrefs
    #links = []
   
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
                # Find the dict in dataaa['files'] that matches this link to get the title
                title = next((t for t, l in dictoflinks.items() if l == link), "")
                if not title:
                    print(f"Title not found for link: {link}")
                    

                #example title is hipermarket_zadar_bleiburskih_zrtava_18_8701_interspar_zadar_0031_20250601_0330.csv
                #the address will be everythin before the last two underscores
                # Split title by underscores to extract address

                parts = title.split("_")


                address = " ".join(parts[:-3])  # All parts except the last two joined by space

                for values in reader:
                    
                    result.append({
                        "datum": dateactual,
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

    max_workers = min(32, (os.cpu_count() or 1) * 5)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_link, dictoflinks.values()))
    for r in results:
        if r is not None:
            data.extend(r)
    return data
    #naziv;šifra;marka;neto količina;jedinica mjere;MPC (EUR);cijena za jedinicu mjere (EUR);MPC za vrijeme posebnog oblika prodaje (EUR);Najniža cijena u posljednjih 30 dana (EUR);sidrena cijena na 2.5.2025. (EUR);barkod;kategorija proizvoda

# import zipfile
# import re

#    return all_data
#if __name__ == "__main__":
    # dateToCrawls = crawlSpar(dateToCrawl=datetime.strptime("29.5.2025", "%d.%m.%Y").date())
    # yesterdays= crawlSpar()
    # print(dateToCrawls == yesterdays)
#     dateToCrawl = datetime.dateToCrawl()
#    # konzum_data = CrawlPlodine()
#     name = "output/eurospin_" + dateToCrawl.strftime("%Y-%m-%d") + ".ndjson"
#     with open(name, "w", encoding="utf-8") as f:
#         for item in konzum_data:
#             f.write(json.dumps(item, ensure_ascii=False) + "\n")
from collections import defaultdict

def crawlPlodine(dateToCrawl=date.today()):
    # https://www.plodine.hr/cjenici/cjenici_02_06_2025_07_00_01.zip
    url = "https://www.plodine.hr/cjenici/cjenici_"+ dateToCrawl.strftime("%d_%m_%Y") + "_07_00_01.zip"
    print(url)
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to download Plodine data for {dateToCrawl.strftime('%d.%m.%Y')}, status code: {response.status_code}")
        return []
    zip_bytes = response.content
    # Extract the csv files from the zip archive
    extracted_files = extract_zip_in_memory(zip_bytes)
    result = []
    dateactual = dateToCrawl.strftime("%d.%m.%Y")
    for filename, content in extracted_files.items():
        print(f"Processing file: {filename}")
          #HIPERMARKET_ANTE_STARCEVICA_21_10290_ZAPRESIC_064_19_02062025015134.csv example filename
        address = filename.replace(".csv","").split("_")
        print(address)
        # Find the index of the last non-digit part before the numeric suffixes
        index = len(address) - 1
        for i in range(len(address) - 1, 0, -1):
            if not address[i].isdigit():
                index = i + 1
                break
        # Join all parts except the first and the trailing numeric parts
        address = " ".join(address[1:index])
        print(f"Extracted address: {address}")

        # Naziv proizvoda;Sifra proizvoda;Marka proizvoda;Neto kolicina;Jedinica mjere;Maloprodajna cijena;Cijena po JM;MPC za vrijeme posebnog oblika prodaje;Najniza cijena u poslj. 30 dana;Sidrena cijena na 2.5.2025;Barkod;Kategorija proizvoda;
        # Decode the content from bytes to string
        text = content.decode('utf-8')
        # Use csv.reader to process the file content after decoding
        reader = csv.reader(io.StringIO(text), delimiter=';')
        header = next(reader, None)  # Skip header
        for values in reader:
            result.append({
                "datum": dateactual,
                "naziv": values[0] if len(values) > 0 else "",  # Naziv proizvoda
                "sifra": values[1] if len(values) > 1 else "",  # Sifra proizvoda
                "marka": values[2] if len(values) > 2 else "",  # Marka proizvoda
                "neto_kolicina": values[3] if len(values) > 3 else "",  # Neto kolicina
                "jedinica_mjere": values[4] if len(values) > 4 else "",  # Jedinica mjere
                "maloprodajna_cijena": values[5] if len(values) > 5 else "",  # Maloprodajna cijena
                "cijena_za_jedinicu_mjere": values[6] if len(values) > 6 else "",  # Cijena po JM
                "maloprodajna_cijena_akcija": values[7] if len(values) > 7 else "",  # MPC za vrijeme posebnog oblika prodaje
                "najniza_cijena": values[8] if len(values) > 8 else "",  # Najniza cijena u poslj. 30 dana
                "sidrena_cijena": values[9] if len(values) > 9 else "",  # Sidrena cijena na 2.5.2025
                "barkod": values[10] if len(values) > 10 else "",  # Barkod
                "kategorije": values[11] if len(values) > 11 else "",  # Kategorija proizvoda
                "adresa": address,
                "trgovina": "Plodine"
            })
        print(f"Processed {len(result)} entries from {filename}")

    return result



def extract_zip_in_memory(zip_bytes):
    extracted = {}
    with libarchive.memory_reader(zip_bytes) as archive:
        for entry in archive:
            chunks = []
            for block in entry.get_blocks():
                chunks.append(block)
            extracted[entry.pathname] = b"".join(chunks)
    return extracted

import zipfile #type: ignore
import tempfile
from io import BytesIO
def crawlStudenac(dateToCrawl=date.today()):
    url = "https://www.studenac.hr/cjenici/PROIZVODI-"#2025-05-31.zip" #29.05.2025

    date_str = dateToCrawl.strftime("%Y-%m-%d")
    dateactual = dateToCrawl.strftime("%d.%m.%Y")
    response = requests.get(url + date_str+".zip")

    result = []
    data = []
    links = []

    #we want to get the a href to get the xml download lin
    zip_bytes = response.content
    files = extract_zip_in_memory(zip_bytes)
    for filename in files:
        text = files[filename].decode('utf-8')
        root = ET.fromstring(text)
        prodajni_objekt = root.find("ProdajniObjekt")
        if prodajni_objekt is not None:
            adresa = prodajni_objekt.findtext("Adresa", default="")
            oblik = prodajni_objekt.findtext("Oblik", default="")
            oznaka = prodajni_objekt.findtext("Oznaka", default="")
            broj_pohrane = prodajni_objekt.findtext("BrojPohrane", default="")
            proizvodi = prodajni_objekt.find("Proizvodi")
            print("radim na",adresa)
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
                        "datum": dateactual,
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
                        "trgovina": "Studenac",
                    })

    return result




import polars as pl #type: ignore
def statisticsCrawl(dateToCrawl=date.today()):
    """Crawls for select statistics, to base inflation on."""
    items = {
    "mlijeko": "3850108020922",
    "jogurt": "3850108037371",
    "sol": "3859888155114",
    "brašno": "3858884172637",
    "šampon": "4005808775163",
    "linolada": "3856020262112",
    "vegeta": "3850104047046",
    "heineken": "3830001714692",
    "maslinovo ulje": "3858882211550"
}
    
   
    spar = crawlSpar()
    ribola = crawlRibola()
    konzum = crawlKonzum()

    # Convert lists to Polars DataFrames with store info
    df_konzum = pl.DataFrame(konzum).with_columns(pl.lit("Konzum").alias("trgovina"))
    df_spar = pl.DataFrame(spar).with_columns(pl.lit("Spar").alias("trgovina"))
    df_ribola = pl.DataFrame(ribola).with_columns(pl.lit("Ribola").alias("trgovina"))

    # Combine all stores
    df_all = pl.concat([df_konzum, df_spar, df_ribola])

    # Filter to only barcodes in items values
    barcodes = list(items.values())
    df_filtered = df_all.filter(pl.col("barkod").is_in(barcodes))

    # Pivot the table so each store's price is a separate column
    df_pivot = df_filtered.pivot(
        values="price",
        index="barcode",
        columns="store"
    )

    # Filter rows where all three stores are present (no nulls)
    df_complete = df_pivot.filter(
        (pl.col("konzum").is_not_null()) &
        (pl.col("spar").is_not_null()) &
        (pl.col("ribola").is_not_null())
    )

    # Map barcode to item name (reverse lookup)
    barcode_to_name = {v: k for k, v in items.items()}

    result = {}
    for row in df_complete.iter_rows(named=True):
        barcode = row["barcode"]
        name = barcode_to_name.get(barcode)
        if not name:
            continue
        result[name] = {
            "konzum": row["konzum"],
            "spar": row["spar"],
            "ribola": row["ribola"]
        }
    return {dateToCrawl.strftime("%d.%m.%Y"):result}

import timeit



def group_addresses_from_json(input_json, output_filepath):
    grouped = defaultdict(lambda: {
        "datum": None,
        "naziv": None,
        "sifra": None,
        "marka": None,
        "neto_kolicina": None,
        "jedinica_mjere": None,
        "maloprodajna_cijena": None,
        "cijena_za_jedinicu_mjere": None,
        "maloprodajna_cijena_akcija": None,
        "najniza_cijena": None,
        "sidrena_cijena": None,
        "barkod": None,
        "kategorije": None,
        "trgovina": None,
        "adresa": []
    })

    for i, item in enumerate(input_json, 1):
        if i % 10000 == 0:
            print(i)
        key = (item["naziv"], item["trgovina"])
        entry = grouped[key]
        if entry["naziv"] is None:
            for k in entry.keys():
                if k != "adresa":
                    entry[k] = item.get(k)
        if item["adresa"] not in entry["adresa"]:
            entry["adresa"].append(item["adresa"])

    with open(output_filepath, 'w', encoding='utf-8') as out:
        for record in grouped.values():
            out.write(json.dumps(record, ensure_ascii=False) + '\n')

def collectioncrawl(dateToCrawl=date.today()):
    """Crawls all stores and returns a combined list which is then saved to a file called output/grouped_<date>.ndjson"""
    if not isinstance(dateToCrawl, date):
        raise ValueError("dateToCrawl must be a datetime.date object")
    date_str = dateToCrawl.strftime("%Y-%m-%d")
    output_filepath = f"output/grouped_{date_str}.ndjson"
    if os.path.exists(output_filepath):
        print(f"File {output_filepath} already exists, skipping crawl.")
        return
    print(f"Crawling data for {dateToCrawl.strftime('%d.%m.%Y')}...")
    if not os.path.exists("output"):
        os.makedirs("output")
    # Crawl each store
    studenac = crawlStudenac(dateToCrawl)
    spar = crawlSpar(dateToCrawl)
    ribola = crawlRibola(dateToCrawl)
    konzum = crawlKonzum(dateToCrawl)
    plodine = crawlPlodine(dateToCrawl)
    print("Done, crawling stores.")
    combined = [*konzum, *spar, *ribola, *studenac, *plodine]
    print(f"Crawled {len(combined)} items from all stores.")
    # Group addresses and save to file
    group_addresses_from_json(combined, output_filepath)
    print(f"Grouped data saved to {output_filepath}")



if __name__ == "__main__":
    # start = timeit.timeit()
    # print(statisticsCrawl())
    # print("trebalo",(timeit.timeit()-start)*1000,"ms")
    # input()
    collectioncrawl()
    # dateToCrawl = datetime.strptime("29.05.2025", "%d.%m.%Y").date()
    # crol = crawlPlodine(dateToCrawl)
    # group_addresses_from_json(crol, f"output/plodine_{dateToCrawl.strftime('%Y-%m-%d')}.ndjson")




