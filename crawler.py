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
            lines = csv_text.splitlines()
            header = next(reader)

            for values in header:
                if len(values) == len(header):
                    entry = dict(zip(header, values))
                    address = ""
                    parsed = urllib.parse.urlparse(link)
                    qs = urllib.parse.parse_qs(parsed.query)
                    title = qs.get("title", [""])[0]
                    # Address is between first and third comma
                    parts = title.split(",")
                    if len(parts) >= 3:
                            address = ",".join(parts[1:3]).strip()
                    #NAZIV PROIZVODA,ŠIFRA PROIZVODA,MARKA PROIZVODA,NETO KOLIČINA,JEDINICA MJERE,MALOPRODAJNA CIJENA,CIJENA ZA JEDINICU MJERE,MPC ZA VRIJEME POSEBNOG OBLIKA PRODAJE,NAJNIŽA CIJENA U POSLJEDNIH 30 DANA,SIDRENA CIJENA NA 2.5.2025,BARKOD,KATEGORIJA PROIZVODA
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



if __name__ == "__main__":
    today = datetime.date.today()
    konzum_data = crawlKonzum(today)
    name = "konzum_" + today.strftime("%Y-%m-%d") + ".ndjson"
    with open(name, "w", encoding="utf-8") as f:
        for item in konzum_data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    

    


