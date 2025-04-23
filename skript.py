# PRODA, Vendula Mráčková, KGI
# SKRIPT - PRVNÍ ČÁST
import pandas as pd  
import glob
import os
import unicodedata
import datetime
import re

os.makedirs("data_senzory/vystupy_excel", exist_ok=True)

# Cesta ke všem CSV souborům ve složce
files = glob.glob("data_senzory/*.csv")

# Dny v týdnu v češtině
dny_tydne_cz = {
    'Monday': 'Pondělí',
    'Tuesday': 'Úterý',
    'Wednesday': 'Středa',
    'Thursday': 'Čtvrtek',
    'Friday': 'Pátek',
    'Saturday': 'Sobota',
    'Sunday': 'Neděle'
}

# Funkce pro úpravu názvů sloupců
def uprav_nazvy_sloupcu(df):
    df.columns = [
        unicodedata.normalize('NFKD', col)
        .encode('ascii', 'ignore').decode('utf-8')
        .replace(' ', '_')
        .replace('.', '_')
        for col in df.columns
    ]
    return df

# Seznam pro ukládání všech výstupů sum_by_day
vsechny_data = []

# Funkce pro zjištění pondělního data
def get_monday_date(date):
    # Pokud není pondělní den, vrátí pondělí předchozího týdne
    return date - datetime.timedelta(days=date.weekday())

# Funkce pro odstranění diakritiky z textu
def remove_diacritics(text):
    return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')

for file in files:
    df = pd.read_csv(file, encoding="utf-8")

    base_name = os.path.basename(file)
    name_without_ext = os.path.splitext(base_name)[0]
    output_file = f"data_senzory/vystupy_excel/{name_without_ext}.xlsx"

    # Rozdělení prvního sloupce na datum a čas
    df[['datum', 'cas']] = df.iloc[:, 0].str.split(',', expand=True)
    df['datum'] = pd.to_datetime(df['datum'], format="%m/%d/%Y")

    # Získání názvu oblasti z názvu sloupce B
    sloupec_b_nazev = df.columns[1]
    posledni_slovo = sloupec_b_nazev.strip().split()[-1]
    nazev_oblasti = posledni_slovo.split('.')[0].capitalize()

    # Agregace podle datumu
    sum_by_day = df.groupby('datum').sum()

    # Den v týdnu + víkend
    sum_by_day['tyden_d'] = sum_by_day.index.day_name().map(dny_tydne_cz)
    sum_by_day['vikend'] = (sum_by_day.index.weekday >= 5).astype(int)

    # Index na datum bez času
    sum_by_day.index = sum_by_day.index.date
    sum_by_day.reset_index(inplace=True)
    sum_by_day.rename(columns={'index': 'datum'}, inplace=True)

    for col in ['cas', '_time']:
        if col in sum_by_day.columns:
            sum_by_day.drop(columns=[col], inplace=True)

    sum_by_day['nazev_oblasti'] = nazev_oblasti
    vsechny_data.append(sum_by_day)

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="data", index=False)
        sum_by_day.to_excel(writer, sheet_name="doprava_dny", index=False)

    print(f" {base_name} ➜ uložen jako {output_file}")

# Souhrnný export + týdenní souhrn
if vsechny_data:
    souhrn_df = pd.concat(vsechny_data, ignore_index=True)

    # Převod na datetime a kontrola
    souhrn_df['datum'] = pd.to_datetime(souhrn_df['datum'], errors='coerce')
    if souhrn_df['datum'].isnull().any():
        print("Některé hodnoty v 'datum' nejsou platné a byly převedeny na NaT.")

    souhrn_df['tyden'] = souhrn_df['datum'].dt.isocalendar().week
    souhrn_df['rok'] = souhrn_df['datum'].dt.isocalendar().year
    souhrn_df['rok_tyden'] = souhrn_df['rok'].astype(str) + '-T' + souhrn_df['tyden'].astype(str).str.zfill(2)

    numeric_cols = souhrn_df.select_dtypes(include='number').columns.tolist()
    for col in ['tyden', 'rok', 'vikend']:
        if col in numeric_cols:
            numeric_cols.remove(col)

    tydenni_souhrn = souhrn_df.groupby(['rok_tyden', 'nazev_oblasti'])[numeric_cols].sum().reset_index()

    souhrn_df = souhrn_df.drop(columns=['vikend'], errors='ignore')
    souhrn_df = uprav_nazvy_sloupcu(souhrn_df)
    tydenni_souhrn = uprav_nazvy_sloupcu(tydenni_souhrn)

    in_columns = [col for col in souhrn_df.columns if 'IN' in col]
    out_columns = [col for col in souhrn_df.columns if 'OUT' in col]

    in_souhrn = souhrn_df.groupby(['rok_tyden', 'nazev_oblasti'])[in_columns].sum().reset_index()
    out_souhrn = souhrn_df.groupby(['rok_tyden', 'nazev_oblasti'])[out_columns].sum().reset_index()

    souhrn_vyjezdy = pd.merge(in_souhrn[['rok_tyden', 'nazev_oblasti'] + in_columns],
                              out_souhrn[['rok_tyden', 'nazev_oblasti'] + out_columns],
                              on=['rok_tyden', 'nazev_oblasti'],
                              how='outer')

    souhrn_vyjezdy['IN'] = souhrn_vyjezdy[in_columns].sum(axis=1)
    souhrn_vyjezdy['OUT'] = souhrn_vyjezdy[out_columns].sum(axis=1)
    souhrn_vyjezdy.drop(columns=in_columns + out_columns, inplace=True)

    souhrn_vyjezdy = uprav_nazvy_sloupcu(souhrn_vyjezdy)

    # Nová část: agregace podle len0-len3 a IN/OUT
agregace_tydny = []

for len_prefix in ['len0', 'len1', 'len2', 'len3']:
    len_in_cols = [col for col in souhrn_df.columns if col.startswith(len_prefix) and 'IN' in col]
    len_out_cols = [col for col in souhrn_df.columns if col.startswith(len_prefix) and 'OUT' in col]

    if len_in_cols or len_out_cols:
        agregace = souhrn_df.groupby(['rok_tyden', 'nazev_oblasti'])[
            len_in_cols + len_out_cols
        ].sum().reset_index()

        agregace[f'{len_prefix}_IN'] = agregace[len_in_cols].sum(axis=1) if len_in_cols else 0
        agregace[f'{len_prefix}_OUT'] = agregace[len_out_cols].sum(axis=1) if len_out_cols else 0

        agregace_final = agregace[['rok_tyden', 'nazev_oblasti', f'{len_prefix}_IN', f'{len_prefix}_OUT']]
        agregace_tydny.append(agregace_final)

# Spojení do jedné tabulky
if agregace_tydny:
    doprava_tydny_len = agregace_tydny[0]
    for df in agregace_tydny[1:]:
        doprava_tydny_len = pd.merge(doprava_tydny_len, df, on=['rok_tyden', 'nazev_oblasti'], how='outer')

    doprava_tydny_len = uprav_nazvy_sloupcu(doprava_tydny_len)

    # ODSTRANĚNÍ ČASU ZE SLOUPCE DATUM
    souhrn_df['datum'] = pd.to_datetime(souhrn_df['datum']).dt.date

    with pd.ExcelWriter("data_senzory/vystupy_excel/souhrn_dopravy.xlsx", engine="openpyxl") as writer:
        souhrn_df.to_excel(writer, sheet_name="doprava_dny", index=False)
        tydenni_souhrn.to_excel(writer, sheet_name="tydenni_souhrn", index=False)
        souhrn_vyjezdy.to_excel(writer, sheet_name="souhrn_vyjezdy", index=False)
        if agregace_tydny:
            doprava_tydny_len.to_excel(writer, sheet_name="doprava_tydny_len", index=False)

    print("Souhrnný soubor s týdenním přehledem ➜ uložen jako data_senzory/vystupy_excel/souhrn_dopravy.xlsx")
else:
    print("Nebyly nalezeny žádné CSV soubory nebo data byla prázdná.")

# SKRIPT – TŘETÍ ČÁST

# Seznam pro přejmenované soubory
prejmenovane_soubory = []

# Přejmenování souborů podle názvu oblasti a pondělního data bez diakritiky
for file in files:
    base_name = os.path.basename(file)
    name_without_ext = os.path.splitext(base_name)[0]

    # Načteme již vytvořený Excel soubor
    output_file = f"data_senzory/vystupy_excel/{name_without_ext}.xlsx"
    
    # Otevřeme Excel soubor a zjistíme pondělní datum a název oblasti
    with pd.ExcelFile(output_file) as xl:
        doprava_dny_df = xl.parse('doprava_dny')
        
        pondeli_datum = doprava_dny_df['datum'].min()
        pondeli_datum = pd.to_datetime(pondeli_datum)
        monday_date = get_monday_date(pondeli_datum)

        nazev_oblasti = doprava_dny_df['nazev_oblasti'].iloc[0]

    # Vytvoření nového názvu souboru
    new_filename = f"data_senzory/vystupy_excel/{remove_diacritics(nazev_oblasti)}_{monday_date.strftime('%Y-%m-%d')}.xlsx"

    if os.path.exists(new_filename):
        os.remove(new_filename)

    os.rename(output_file, new_filename)

    # Přidání do seznamu
    prejmenovane_soubory.append(new_filename)

    print(f"Soubor {output_file} byl přejmenován na {new_filename}")

# Složka s přejmenovanými soubory
sloucena_data = []

for soubor in prejmenovane_soubory:  # Používáme pouze přejmenované soubory
    try:
        df = pd.read_excel(soubor, sheet_name="data")

        base_name = os.path.basename(soubor)
        nazev_bez_pripony = os.path.splitext(base_name)[0]

        df["soubor"] = nazev_bez_pripony
        sloucena_data.append(df)

    except Exception as e:
        print(f"Nelze načíst list 'data' ze souboru {soubor}: {e}")

# Spojení všech dat do jednoho dataframe 
if sloucena_data:
    prubeh_df = pd.concat(sloucena_data, ignore_index=True)

    # Odstranění sloupce 'soubor', pokud existuje
    if 'soubor' in prubeh_df.columns:
        prubeh_df.drop(columns='soubor', inplace=True)

    # Nahrazení NaN hodnot nulami
    prubeh_df.fillna(0, inplace=True)

    # Převod 'datum' a 'cas' na stringy a spojení do 'date_time'
    prubeh_df['date_time'] = prubeh_df['datum'].astype(str) + ' ' + prubeh_df['cas'].astype(str)

    # Převedení 'date_time' na datetime objekt
    prubeh_df['date_time'] = pd.to_datetime(prubeh_df['date_time'], errors='coerce')

    # Seřazení podle 'date_time'
    prubeh_df = prubeh_df.sort_values(by='date_time')

    # Uložení do Excelu
    vystupni_soubor = "data_senzory/vystupy_excel/prubeh_dopravy.xlsx"
    prubeh_df.to_excel(vystupni_soubor, index=False)

    print(f"Spojený Excel uložen jako {vystupni_soubor}")

else:
    print("Nebyla načtena žádná data pro spojení.")

    
# Spojený Excel uložen jako "prubeh_dopravy.xlsx"
# TADY ZAČÍNÁ PŘIDANÝ DRUHÝ SKRIPT

print("Začíná tvorba listu 'doprava_join'...")

# Cesta k již uloženému souboru s vývojem dopravy
soubor = 'data_senzory/vystupy_excel/prubeh_dopravy.xlsx'

# Načtení dat z výchozího listu (původně 'Sheet1' → můžeš změnit na 'data' pokud je potřeba)
try:
    vystup = pd.read_excel(soubor, sheet_name='Sheet1')
except:
    vystup = pd.read_excel(soubor, sheet_name=0)  # fallback pro první list

# Vytažení kombinací oblastí a směrů pro Šmeralova
pattern = r"(Šmeralova(?:\d*)\.(?:IN|OUT))"
columns_to_split = [col for col in vystup.columns if re.search(pattern, col)]

# Základní (neměřené) sloupce
base_cols = [col for col in vystup.columns if col not in columns_to_split]

# Pro každou kombinaci oblast.smer
vsechny_df = []
for match in set(re.findall(pattern, " ".join(columns_to_split))):
    matched_cols = [col for col in columns_to_split if match in col]
    subset = vystup[base_cols + matched_cols].copy()
    new_colnames = {col: col.replace(f" {match}", "") for col in matched_cols}
    subset.rename(columns=new_colnames, inplace=True)
    subset["senzor"] = match
    vsechny_df.append(subset)

# Spojení do jednoho DataFrame
slouceny_df = pd.concat(vsechny_df, ignore_index=True)

# Odstranění sloupců 'datum' a 'cas', pokud existují
slouceny_df.drop(columns=[col for col in ['datum', 'cas'] if col in slouceny_df.columns], inplace=True)

# Výpočet souhrnné dopravy ze sloupců len0 až len3
soucetove_sloupce = [col for col in slouceny_df.columns if col.startswith("len")]
slouceny_df["sum_doprava"] = slouceny_df[soucetove_sloupce].sum(axis=1)

# Uložení do nového listu 'doprava_join' v původním Excelu
with pd.ExcelWriter(soubor, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
    slouceny_df.to_excel(writer, sheet_name='doprava_join', index=False)

print("List 'doprava_join' byl úspěšně vytvořen a uložen.")