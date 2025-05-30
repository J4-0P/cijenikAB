# {
#     "datum": "29.05.2025",
#     "naziv": "SIR NATU.TO.140g PRE",
#     "sifra": "0087677",
#     "marka": "",
#     "neto_kolicina": "",
#     "jedinica_mjere": "KOM",
#     "maloprodajna_cijena": "1.99",
#     "cijena_za_jedinicu_mjere": "14.21",
#     "maloprodajna_cijena_akcija": "",
#     "najniza_cijena": "",
#     "sidrena_cijena": "1.99",
#     "barkod": "3850355001750",
#     "kategorije": "HRANA",
#     "adresa": "Cesta dr. Franje Tuđmana 7 Kastel Sucurac"
#   }
#example element of list of JSON file
import json
import polars as pl # type: ignore
import zipfile
import gzip

def gzip_file(input_path, output_gzip_path):
    with open(input_path, 'rb') as f_in:
        with gzip.open(output_gzip_path, 'wb') as f_out:
            f_out.writelines(f_in)
def gunzip_file(input_gzip_path):
    with gzip.open(input_gzip_path, 'rb') as f_in:
        return f_in.read()


#zip_file("output.ndjson", "output.zip")



if False:
    print("idemo")
    import timeit
    start_time = timeit.default_timer()
    print(find_first_by_characteristic("barkod", "3850355001750"))
    print("Time taken:", (timeit.default_timer() - start_time)*1000)

def find_all_occurences_by_characteristic(characteristic, value, file, file2, df):
    df = pl.read_json(file)
    mask = df[characteristic] == value
    filtered = df.filter(mask)
    if filtered.height > 0:
        return [dict(zip(filtered.columns, row)) for row in filtered.rows()]
    return []

def sum_up_every_price(characteristic, value, file, file2, df):
    df = pl.read_json(file)
    suma = 0
    already_checked = set()
    for row in df.rows():
        if row[1] not in already_checked:
            already_checked.add(row[1])
            price_str = row[7] if row[5] == "KOM" else row[6]
            if price_str and price_str.strip():
                suma += float(price_str)
    return suma


def find_price_discrepancies(characteristic, value, file, file2, df):
    df = pl.read_json(file)
    seen = set()
    discrepancies = 0

    # Group by barkod and collect all unique prices for each barkod
    barkod_groups = df.group_by("barkod").agg(pl.col("cijena_za_jedinicu_mjere").unique())
    discrepancies = 0
    for group in barkod_groups.rows():
        barkod = group[0]
        unique_prices = [p for p in group[1] if p and str(p).strip()]
        if len(unique_prices) > 1:
            ime = df.filter(df["barkod"] == barkod)["naziv"][0]
            discrepancies += 1

    return discrepancies/len(barkod_groups.rows())


#print(len(find_all_occurences_by_characteristic("barkod", "3850355001750")))
#print(sum_up_every_price("output.json"))
#import timeit
#start_time = timeit.default_timer()
#print(find_price_discrepancies("output.json"))
#print("Time taken:", (timeit.default_timer() - start_time)*1000) #14158.267299997533
import json

def json_to_ndjson(input_file="input.json", output_file="output.ndjson", df=None):
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    with open(output_file, "w", encoding="utf-8") as f:
        for entry in data:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

# json_to_ndjson("output.json", "output.ndjson")

def optimalDiscrepancySearch(characteristic, value, file, file2, df):
    if df is None:
        df = pl.read_ndjson(file2).select(["barkod", "cijena_za_jedinicu_mjere"])
    else:
        df = df.select(["barkod", "cijena_za_jedinicu_mjere"])
    discrepancy_df = (
        df.group_by("barkod")
          .agg(pl.col("cijena_za_jedinicu_mjere").n_unique().alias("unique_price_count"))
          .filter(pl.col("unique_price_count") > 1)
    )
    total_unique_barkod = df.select("barkod").n_unique()
    return discrepancy_df.height / total_unique_barkod


def find_first_by_characteristic(characteristic, value, file, file2, df):
    data = pl.read_json(file)
    mask = data[characteristic] == value
    filtered = data.filter(mask)
    if filtered.height > 0:
        return dict(zip(filtered.columns, filtered.row(0)))
    return None

def optimal_find_first_by_characteristic(characteristic, value, file, file2, df):
    if df is None:
        df = pl.read_ndjson(file2)
    else:
        df = df
    df = df.filter(pl.col(characteristic) == value)
    return df.row(0, named=True) if df.height > 0 else None


def sum_up_every_price_fast(characteristic, value, file, file2, df):
    if df is None:
        df = pl.read_ndjson(file2)
    else:
        df = df
    df = df.unique(subset=["barkod"])
    def safe_float_column(col_name):
        return (
            pl.col(col_name)
            .str.strip_chars()
            .replace("", None)
            .cast(pl.Float64, strict=False)
        )
    maloprodajna = safe_float_column("maloprodajna_cijena")
    cijena_po_jedinici = safe_float_column("cijena_za_jedinicu_mjere")
    df = df.with_columns([
        pl.when(pl.col("jedinica_mjere") == "KOM")
          .then(maloprodajna)
          .otherwise(cijena_po_jedinici)
          .alias("price")
    ])
    return df.select(pl.col("price").drop_nulls().sum()).collect()[0, 0]

import timeit
# print("krećem")
# start_time = timeit.default_timer()
# print("Slow discrepancy search result:", find_price_discrepancies("output.json"))
# time_taken = (timeit.default_timer() - start_time) * 1000
# print("Time taken:", time_taken)
# print()

def timeTwoFunctions(func1, func2, characteristic, value, file, file2, df):
    import timeit
    start_time = timeit.default_timer()
    result1 = func1(characteristic, value, file, file2, df)
    time1 = (timeit.default_timer() - start_time) * 1000

    start_time = timeit.default_timer()
    result2 = func2(characteristic, value, file, file2, df).replace("\n", "")
    time2 = (timeit.default_timer() - start_time) * 1000
    print()
    print(f"Function 1 took {time1} ms")
    print(f"Function 2 took {time2} ms")
    print(f"Function 1 result: {result1}")
    print(f"Function 2 result: {result2}")

    if result1 == result2:
        print("OK")
    else:
        print("discrep")
    print()
    print(f"Speed increase: {time1/time2}x")
    print("--------------------------------")
    print()
    return result1, result2

def optimal_find_all_occurences_by_characteristic(characteristic, value, file, file2, df):
    if df is None:
        df = pl.read_ndjson(file2)
    else:
        df = df
    df = df.filter(pl.col(characteristic) == value).collect()
    return df.to_dicts() if df.height > 0 else []

from functools import reduce
import operator



def optimal_find_by_characteristics(dic, file, df):
    df = pl.read_ndjson(file)
    mask = pl.all([pl.col(c) == v for c, v in dic.items()])
    filtered = df.filter(mask)
    return filtered.to_dicts() if filtered.height > 0 else []
import re
def find(filters: dict, df):
    conditions = []
    for col, val in filters.items():
        if isinstance(val, dict):  # Comparison or pattern
            if "lt" in val:
                conditions.append(pl.col(col).cast(pl.Float64, strict=False) < val["lt"])
            if "gt" in val:
                conditions.append(pl.col(col).cast(pl.Float64, strict=False) > val["gt"])
            if "le" in val:
                conditions.append(pl.col(col).cast(pl.Float64, strict=False) <= val["le"])
            if "ge" in val:
                conditions.append(pl.col(col).cast(pl.Float64, strict=False) >= val["ge"])
            if "eq" in val:
                # cast to float for numeric equality comparison
                conditions.append(pl.col(col).cast(pl.Float64, strict=False) == val["eq"])
            if "exclude" in val:
                conditions.append(~pl.col(col).str.to_lowercase().str.contains(val["exclude"].lower(), literal=True))
            contains = val["contains"]
            if isinstance(contains, str):
                print("JE INSTANCE")
                contains = [contains]
                if not any(len(s) >= 3 for s in contains):
                     print("pre mali ti je frere")
                     return []
      

            col_exprs = [
                pl.col(col)
                .cast(str)
                .str.to_lowercase()
                .str.contains(re.escape(contain.lower()), literal=True)
                for contain in contains
            ]
            conditions.append(reduce(operator.or_, col_exprs))

        else:  # Exact match
            conditions.append(pl.col(col) == val)

    if not conditions:
        return []

    
    mask = reduce(operator.and_, conditions)
    filtered = df.filter(mask).collect()
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
    data = filtered.to_dicts()
    for item in data:
        key = (item["naziv"], item["trgovina"])
        entry = grouped[key]
        # Set other fields once (assuming they're identical or you want the first)
        if entry["naziv"] is None:
            for k in entry.keys():
                if k != "adresa":
                    entry[k] = item.get(k)
        # Append unique addresses only
        if item["adresa"] not in entry["adresa"]:
            entry["adresa"].append(item["adresa"])
        for item in data:
            key = (item["naziv"], item["trgovina"])
            entry = grouped[key]
            # Set other fields once (assuming they're identical or you want the first)
            if entry["naziv"] is None:
                for k in entry.keys():
                    if k != "adresa":
                        entry[k] = item.get(k)
            # Append unique addresses only
            if item["adresa"] not in entry["adresa"]:
                entry["adresa"].append(item["adresa"])
    
    filtered = df.filter(mask).collect()
    returnal = filtered.to_dicts()
    if len(returnal) > 1000: 
        print(filters,"je preee dugo, čak",len(returnal))
        return []
    return returnal if filtered.height > 0 else []
from collections import defaultdict
def find_all(filters: dict, df):
    conditions = []
    for col, val in filters.items():
        if isinstance(val, dict):  # Comparison or pattern
            if "lt" in val:
                conditions.append(pl.col(col).cast(pl.Float64, strict=False) < val["lt"])
            if "gt" in val:
                conditions.append(pl.col(col).cast(pl.Float64, strict=False) > val["gt"])
            if "le" in val:
                conditions.append(pl.col(col).cast(pl.Float64, strict=False) <= val["le"])
            if "ge" in val:
                conditions.append(pl.col(col).cast(pl.Float64, strict=False) >= val["ge"])
            if "eq" in val:
                conditions.append(pl.col(col).cast(pl.Float64, strict=False) == val["eq"])

            if "contains" in val:
                for contain in val["contains"]:
                    conditions.append(
                        pl.col(col).str.to_lowercase().str.contains(contain.lower(), literal=True)
                    )
        else:  # Exact match
            conditions.append(pl.col(col) == val)

    if not conditions:
        return []

    mask = reduce(operator.and_, conditions)
    filtered = df.filter(mask).collect()
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
    data = filtered.to_dicts()
    for item in data:
        key = (item["naziv"], item["trgovina"])
        entry = grouped[key]
        # Set other fields once (assuming they're identical or you want the first)
        if entry["naziv"] is None:
            for k in entry.keys():
                if k != "adresa":
                    entry[k] = item.get(k)
        # Append unique addresses only
        if item["adresa"] not in entry["adresa"]:
            entry["adresa"].append(item["adresa"])
        for item in data:
            key = (item["naziv"], item["trgovina"])
            entry = grouped[key]
            # Set other fields once (assuming they're identical or you want the first)
            if entry["naziv"] is None:
                for k in entry.keys():
                    if k != "adresa":
                        entry[k] = item.get(k)
            # Append unique addresses only
            if item["adresa"] not in entry["adresa"]:
                entry["adresa"].append(item["adresa"])

    # Convert grouped dict back to list
    return list(grouped.values())
    return filtered.to_dicts() if filtered.height > 0 else []

def minimal_unique_prefix_length(strings):
    if not strings:
        return 0

    max_len = max(len(s) for s in strings)

    for n in range(1, max_len + 1):
        prefixes = [s[:n] for s in strings]
        if len(set(prefixes)) == len(strings):
            return n
    return max_len  # fallback if no smaller prefix can distinguish all strings

def shorten_strings_to_unique_prefix(strings):
    n = minimal_unique_prefix_length(strings)
    return [s[:n] for s in strings]



def count_all_by_barcode(df):

    return df.select(pl.col("kategorije")).collect().unique().to_dict()["kategorije"].to_list()

import json
from collections import defaultdict

def group_addresses_from_file(input_filepath, output_filepath):
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

    with open(input_filepath, 'r', encoding='utf-8') as f:
        i = 0
        for line in f:
            i+=1
            if i%10000==0:
                print(i)
            item = json.loads(line)
            key = (item["naziv"], item["trgovina"])
            entry = grouped[key]
            if entry["naziv"] is None:
                for k in entry.keys():
                    if k != "adresa":
                        entry[k] = item.get(k)
            if item["adresa"] not in entry["adresa"]:
                entry["adresa"].append(item["adresa"])

    # Write combined output as NDJSON
    with open(output_filepath, 'w', encoding='utf-8') as out:
        for record in grouped.values():
            out.write(json.dumps(record, ensure_ascii=False) + '\n')

if __name__ == "__main__":
    print(count_all_by_barcode(pl.scan_ndjson("output/ribola_2025-05-29.ndjson")))
    #print(shorten_strings_to_unique_prefix(count_all_by_barcode(pl.scan_ndjson("output/ribola_2025-05-29.ndjson"))))
    #group_addresses_from_file("output/combined_2025-05-30.ndjson","output/grouped_2025-05-30.ndjson")

  