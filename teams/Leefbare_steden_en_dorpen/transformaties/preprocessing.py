import pandas as pd
from helpers import (
    corrigeer_bu_codes,
    filter_limburgse_buurten,
    map_bu_code_naar_corop_code,
    laad_en_verwerk_enkel_invoerbestand
)
from typing import List, Union
import re

def transformeer_woononderzoek_nederland(df, geolevel):
    """
    Verwerkt het woononderzoek DataFrame en transformeert het naar het gewenste formaat.
    
    Args:
    - df (pd.DataFrame): Het originele woononderzoek DataFrame.
    - geolevel (str): Niveau van de geografische indeling (bv. 'corop_id').
    
    Returns:
    - pd.DataFrame: Het getransformeerde, opgeschoonde DataFrame.
    """
    # Veiligheid: kopie van het dataframe maken
    df_transformed = df.copy()
    
    # Reset de index, hernoem 'Provincies' naar 'geoitem'
    if geolevel == 'nederland':
        df_transformed = df_transformed.reset_index(drop=True).rename(columns={geolevel.title(): "geoitem"})
    else:
        df_transformed = df_transformed.reset_index(drop=True).rename(columns={"Provincies": "geoitem"})

    # Stap 1: Gebruik `.melt()` om de kolomwaarden te transformeren naar rijen
    df_melted = df_transformed.melt(id_vars=["geoitem"], var_name="categorien", value_name="MO_11b")

    # Stap 2: Splits de kolomnamen (in het veld 'categorien') op '|' in meerdere dimensies
    categorien_split = df_melted["categorien"].str.split(r"\|", expand=True)
    
    # Zet correcte kolomnamen
    categorien_split.columns = ["nvt", "dim_tevredenheid_1", "period"]

    # Stap 3: Voeg de gesplitste kolommen toe aan de DataFrame
    df_melted = pd.concat([df_melted, categorien_split], axis=1)

    # Stap 4: Opschonen - verwijder de onnodige kolommen
    df_melted = df_melted.drop(columns=["categorien", "nvt"])

    # Stap 5: Verwijder lege of niet-relevante rijen
    df_melted = df_melted.dropna(subset=["period", "dim_tevredenheid_1"])

    # Stap 6: Zorg dat alleen relevante kolommen en waarden overblijven
    df_melted = df_melted[df_melted["MO_11b"] != "-"]

    # Stap 7: Converteer 'MO_11b' naar numerieke waarden (voor consistentie)
    # Vervang foutieve of niet-converteerbare waarden door 0
    df_melted["MO_11b"] = pd.to_numeric(df_melted["MO_11b"], errors="coerce").fillna(0).astype(int)

    # # Stap 8: Map de waarden van 'dim_eigendom_1' naar de opgegeven codes
    # eigendom_mapping = {
    #     'Eigenaar-bewoner': '2',
    #     'Private huur': '12',
    #     'Corporatiehuur': '11'
    # }
    # df_melted["dim_eigendom_1"] = df_melted["dim_eigendom_1"].map(eigendom_mapping)

    # Stap 9: Transformeer 'dim_tevredenheid_1' door deze kleinere letters te maken, spaties te vervangen en komma's te verwijderen
    df_melted["dim_tevredenheid_1"] = (
        df_melted["dim_tevredenheid_1"]
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(",", "")
    )

    # Stap 10: Herdefinieer de 'geoitem'-code via mapping voor COROP-gebieden
    if geolevel == 'nederland':
        geoitem_mapping = {'Nederland': 'nl00'}
    else:
        geoitem_mapping = {
            "Limburg": "pv31"
        }
    # Pas de mapping toe
    df_melted['geoitem'] = df_melted['geoitem'].map(geoitem_mapping)

    # Stap 11: Voeg geolevel-kolom toe
    df_melted['geolevel'] = geolevel

    return df_melted

def transformeer_woonderzoek_nederland(df, n_rows, vermenigvuldig_met_100=True):
    """
    Transformeert het DataFrame van het woningonderzoek Nederland naar een gestructureerd formaat.

    Deze functie voert de volgende stappen uit:
    1. Verwijdert onnodige rijen en kolommen
    2. Identificeert de jaartallen uit de data
    3. Hernoemt de kolommen naar de geïdentificeerde jaartallen
    4. Smelt het DataFrame om een lange formaat te creëren
    5. Voegt een 'Regio' kolom toe met de waarde 'Nederland'
    6. Filtert ongeldige rijen
    7. Vermenigvuldigt optioneel de 'Waarde' kolom met 100 om percentages om te zetten naar hele getallen

    Args:
        df (pd.DataFrame): Origineel DataFrame ingeladen vanuit het Excel-bestand.
        n_rows (int): Aantal rijen om te behouden voor verwerking.
        vermenigvuldig_met_100 (bool, optional): Indien True, vermenigvuldigt de 'Waarde' kolom met 100. Standaard is True.

    Returns:
        pd.DataFrame: Getransformeerd DataFrame met kolommen 'Regio', 'Jaartal', 'Categorie' en 'Waarde'.
    """
    # Stap 1: Verwijder onnodige rijen en kolommen
    df = df.iloc[:n_rows, :6]  # Behoud de eerste n_rows rijen en 6 kolommen

    # Stap 2: Identificeer de jaartallen uit de data
    df.set_index(df.columns[0], inplace=True)
    jaartallen = pd.to_numeric(df.iloc[0], errors='coerce').dropna().astype(int).tolist()

    # Stap 3: Hernoem de kolommen naar de geïdentificeerde jaartallen
    df.columns = jaartallen
    df.reset_index(inplace=True)
    df.rename(columns={df.columns[0]: 'Categorie'}, inplace=True)

    # Stap 4: Smelt het DataFrame
    df_melted = df.melt(id_vars=['Categorie'], var_name='Jaartal', value_name='Waarde')
    df_melted['Waarde'] = pd.to_numeric(df_melted['Waarde'], errors='coerce')

    # Stap 5: Voeg een 'Regio' kolom toe
    df_melted['Regio'] = 'Nederland'

    # Stap 6: Filter de ongeldige rijen eruit
    df_melted = df_melted[df_melted['Categorie'].notna() & (df_melted['Categorie'] != df_melted['Waarde'])]

    # Stap 7: Vermenigvuldig optioneel de 'Waarde' kolom met 100
    if vermenigvuldig_met_100:
        df_melted['Waarde'] = df_melted['Waarde'] * 100

    # Herorden de kolommen
    df_melted = df_melted[['Regio', 'Jaartal', 'Categorie', 'Waarde']]

    return df_melted

def transformeer_woonderzoek_data(df, region_mapping, column_renames=None):
    """
    Transformeert het gecombineerde woonderzoek DataFrame.
    Deze functie voert de volgende stappen uit:
    1. Voegt een 'geolevel' kolom toe.
    2. Mapt de regio's naar hun respectievelijke codes.
    3. Hernoemt de kolommen volgens de gespecificeerde mapping.
    4. Hernoemt 'Regio' naar 'geoitem'.
    5. Hernoemt 'Jaartal' naar 'period' (optioneel).
    6. Voorziet dimensie-item-kolommen (dim_*) van gestandaardiseerde waarden in lowercase
       zonder spaties.

    Args:
        df (pd.DataFrame): Gecombineerd DataFrame van Limburg en Nederland woonderzoek
        region_mapping (dict): Mapping van regio namen naar hun codes
        column_renames (dict): Mapping van oude kolomnamen naar nieuwe kolomnamen

    Returns:
        pd.DataFrame: Getransformeerd DataFrame
    """
    # Stap 1: Voeg 'geolevel' kolom toe
    df['geolevel'] = df['Regio'].map(lambda x: "nederland" if x == "Nederland" else "corop_id")

    # Stap 2: Map de regio's naar hun codes
    df['Regio'] = df['Regio'].map(region_mapping)

    # Stap 3: Rename de Regio kolom naar geoitem
    df.rename(columns={'Regio': 'geoitem'}, inplace=True)

    # Stap 4: Rename de Jaartal kolom naar period (optioneel)
    if "Jaartal" in df.columns:
        df.rename(columns={'Jaartal': 'period'}, inplace=True)

    # Stap 5: Hernoem de overige kolommen (optioneel)
    if column_renames:
        df = df.rename(columns=column_renames)

    # Stap 6: Hernoem dimensie-items naar lowercase en verwijder spaties
    for col in df.columns:
        if col.startswith('dim_'):
            df[col] = df[col].apply(lambda x: str(x).lower().replace(" ", "_").replace(",", "") if isinstance(x, str) else x)

    return df

def transformeer_leefbarometer_data(
    input_bestand_pad,
    limburg_buurten_pad,
    column_renames,
    relevante_jaren,
    bu_code_correcties_pad,
    regio_mapping
):
    """
    Verwerkt Leefbarometer data voor Limburgse COROP-regio's.

    Deze functie voert de volgende stappen uit:
    1. Laad ruwe data
    2. Corrigeer buurtcodes
    3. Filter op Limburgse buurten
    4. Voeg COROP-codes toe
    5. Aggregeer data op COROP-niveau
    6. Filter op relevante jaren
    7. Hernoem kolommen naar de juiste indicatorcodes

    Parameters:
    input_bestand_pad (str): Pad naar het input CSV-bestand met Leefbarometer scores.
    bu_code_correcties_pad (str): Pad naar het Excel-bestand met buurtcode correcties.
    limburg_buurten_pad (str): Pad naar het Excel-bestand met Limburgse buurtcodes.
    column_renames (dict): Woordenboek om kolomnamen te hernoemen naar de juiste indicatorcodes.
    relevante_jaren (list): Lijst van jaren om op te nemen in de output.
    regio_mapping (dict): Woordenboek om COROP-namen te mappen naar gewenste output namen. Standaard is None.

    Returns:
    pandas.DataFrame: Verwerkte Leefbarometer data geaggregeerd op COROP-niveau.
    """
    # Stap 1: Laad ruwe data
    df_lbm_buurt = pd.read_csv(input_bestand_pad)
    
    # Stap 2: Corrigeer buurtcodes
    df_lbm_buurt = corrigeer_bu_codes(
        df=df_lbm_buurt, 
        bu_code_kolom="bu_code", 
        pad_naar_bu_code_correcties=bu_code_correcties_pad
    )
    
    # Stap 3: Filter op Limburgse buurten
    df_lbm_buurt = filter_limburgse_buurten(
        df=df_lbm_buurt, 
        buurt_code_kolom="bu_code", 
        pad_naar_limburgse_buurten=limburg_buurten_pad
    )
    
    # Stap 4: Voeg COROP-codes toe
    df_lbm_buurt = map_bu_code_naar_corop_code(
        df=df_lbm_buurt, 
        buurt_code_kolom="bu_code", 
        pad_naar_limburgse_buurten=limburg_buurten_pad
    )
    
    # Stap 5: Aggregeer data op COROP-niveau
    gegroepeerde_data_limburg = df_lbm_buurt.groupby(['COROP_NAAM', 'jaar'], as_index=False).agg({
        'lbm': 'mean',  # MO_10a: Gemiddelde leefbaarheidsscore
        'fys': 'mean',  # D_39a: Gemiddelde score fysieke omgeving
        'vrz': 'mean'   # D_39aa: Gemiddelde score voorzieningen
    })
    
    # Stap 6: Filter op relevante jaren
    gegroepeerde_data_limburg = gegroepeerde_data_limburg[gegroepeerde_data_limburg['jaar'].isin(relevante_jaren)]
    
    # Stap 7: Hernoem kolommen naar de juiste indicatorcodes
    gegroepeerde_data_limburg = gegroepeerde_data_limburg.rename(columns=column_renames)
    gegroepeerde_data_limburg['geoitem'] = gegroepeerde_data_limburg['geoitem'].map(regio_mapping)
    
    gegroepeerde_data_limburg['geolevel'] = 'corop_id'
    
    return gegroepeerde_data_limburg

def laad_woningtekort_data(regio_mapping):
    # 2025
    df_2025 = pd.read_excel('../../../data/Woningtekort/Woningtekort - 2025 - COROP-gebieden.xlsx', skiprows=1)
    df_2025 = df_2025.iloc[:3, :]
    df_2025.columns = ['Regio', 'aantal']
    df_2025['period'] = '2025'
    df_2025['aantal'] = df_2025['aantal'].replace("%", "")
    df_2025['aantal'] =  df_2025['aantal'].astype(float).abs() * 100

    # 2024
    df_2024 = pd.read_excel('../../../data/Woningtekort/Woningtekort - 2024 - COROP-gebieden.xlsx', skiprows=1)
    df_2024 = df_2024.iloc[:3, :]
    df_2024.columns = ['Regio', 'aantal']
    df_2024['period'] = '2024'
    df_2024['aantal'] = df_2024['aantal'].replace("%", "")
    df_2024['aantal'] =  df_2024['aantal'].astype(float).abs() * 100

    # 2023
    # Inlezen van de data
    df_2023 = pd.read_excel(
        '../../../data/Woningtekort/Woningtekort - COROP-gebieden 2023.xlsx'
    ).iloc[[2], 1:4]

    df_2023.columns = ['Noord-Limburg', 'Midden-Limburg', 'Zuid-Limburg']

    # Data in lang formaat zetten (melt)
    df_2023 = df_2023.melt(var_name='Regio', value_name='woningtekort')

    # Voeg de periode toe
    df_2023['period'] = '2023'

    # Voeg woningvoorraad waardes toe uit Woningmonitor 2024
    df_2023['woningvoorraad'] = [296021, 111531, 127375]

    # Bereken het percentage woningtekort
    df_2023['aantal'] = abs((df_2023['woningtekort'] / df_2023['woningvoorraad']) * 100)

    # Drop de onnodige kolommen
    df_2023.drop(['woningtekort', 'woningvoorraad'], axis=1, inplace=True)



    # 2022
    df_2022 = pd.read_excel('../../../data/Woningtekort/Actueel woningtekort Primos 2022.xlsx').iloc[:3, [0, 2]]
    df_2022 = df_2022.rename(columns={'Unnamed: 0': 'Regio', 'Woningtekort 2022 (%)': 'aantal'})
    df_2022['period'] = '2022'
    df_2022['aantal'] =  df_2022['aantal'].astype(float).abs() * 100

    # 2021
    df_2021 = pd.read_excel('../../../data/Woningtekort/Actueel woningtekort Primos 2021.xlsx', sheet_name='Actueel woningtekort').iloc[:3, [0, 2]]
    df_2021 = df_2021.rename(columns={'Unnamed: 0': 'Regio', 'Actueel woningtekort (%)': 'aantal'})
    df_2021['period'] = '2021'
    df_2021['aantal'] =  df_2022['aantal'].astype(float)

    # 2019
    # 2019 Woningvoorraad
    df_2019_woningvoorraad = pd.read_excel(
        '../../../data/Woningtekort/Primos 2019 Ontwikkeling woningvoorraad  - NL Limburg COROP-gebieden.xls'
    ).iloc[[3], [1, 5, 9, 13]]
    df_2019_woningvoorraad.columns = ['Noord-Limburg', 'Midden-Limburg', 'Zuid-Limburg', 'Nederland']
    df_2019_woningvoorraad = df_2019_woningvoorraad.melt(var_name='Regio', value_name='woningvoorraad')

    # 2019 Woningbehoefte
    df_2019_woningbehoefte = pd.read_excel(
        '../../../data/Woningtekort/Primos 2019 woningbehoefte  - NL Limburg COROP-gebieden 2019.xls'
    ).iloc[[3], [5, 10, 15, 20]]
    df_2019_woningbehoefte.columns = ['Noord-Limburg', 'Midden-Limburg', 'Zuid-Limburg', 'Nederland']
    df_2019_woningbehoefte = df_2019_woningbehoefte.melt(var_name='Regio', value_name='woningbehoefte')

    # Samenvoegen van de dataframes
    df_2019 = pd.merge(df_2019_woningvoorraad, df_2019_woningbehoefte, on='Regio')
    df_2019['period'] = '2019'

    # Berekening van het tekort
    df_2019['aantal'] = ((df_2019['woningbehoefte'] - df_2019['woningvoorraad']) / df_2019['woningvoorraad']) * 100

    # Drop onnodige kolommen
    df_2019.drop(['woningvoorraad', 'woningbehoefte'], axis=1, inplace=True)

    
    
    ### Voeg missende Nederland waardes toe
    dict_ned = {
        2020: 4.2, # https://www.rijksoverheid.nl/actueel/nieuws/2020/06/15/staat-van-de-woningmarkt-2020
        2021: 3.5, # https://www.volkshuisvestingnederland.nl/actueel/nieuws/2021/07/05/staat-van-de-woningmarkt-2021-woningmarkt-oververhit
        2022: 3.9, # https://www.rijksoverheid.nl/actueel/nieuws/2023/07/12/woningbouwopgave-stijgt-naar-981.000-tot-en-met-2030 
        2023: 4.8, # https://www.rijksoverheid.nl/actueel/nieuws/2023/07/12/woningbouwopgave-stijgt-naar-981.000-tot-en-met-2030
        2024: 4.9, # https://www.rijksoverheid.nl/actueel/nieuws/2024/07/12/seinen-op-groen-om-jaarlijks-100.000-nieuwe-woningen-te-bouwen
        2025: 4.8, # https://abfresearch.nl/publicaties/primos-prognose-2025/
    }

    # Voeg Nederland-waarden dynamisch toe aan een DataFrame
    df_nederland = pd.DataFrame(
        [{'Regio': 'Nederland', 'period': year, 'aantal': value} for year, value in dict_ned.items()]
    )

    # Combine the datasets into a single DataFrame
    df = pd.concat([df_2019, df_2021, df_2022, df_2023, df_2024, df_2025, df_nederland], ignore_index=True)
    df['period'] = pd.to_numeric(df['period'], errors='coerce').astype('Int64')

    # Transformeer de data
    df_mo_11a = transformeer_woonderzoek_data(df, regio_mapping)

    df_mo_11a = df_mo_11a.rename(columns={'aantal': 'mo_11a'})

    return df_mo_11a

def laad_data_invoerapplicatie(bron_bestanden: Union[str, List[str]]) -> pd.DataFrame:
    """
    Laadt en verwerkt één of meerdere bronbestanden, combineert resultaten in één DataFrame.
    
    Parameters:
        bron_bestanden (str of list van str): Eén of meerdere paden naar bestanden.
    
    Returns:
        pd.DataFrame: Gecombineerde resultaten.
    """
    # Sta toe dat er een string mee wordt gegeven
    if isinstance(bron_bestanden, str):
        bron_bestanden = [bron_bestanden]

    # Verwerk alle bestanden en voeg samen
    alle_df = []
    for bestand in bron_bestanden:
        df = laad_en_verwerk_enkel_invoerbestand(bestand)
        alle_df.append(df)

    # Combineer alles tot één DataFrame
    if alle_df:
        resultaat = pd.concat(alle_df, ignore_index=True)
    else:
        resultaat = pd.DataFrame()

    return resultaat

# Functie om CBS data verder te transformeren
def transformeer_cbs_data(df, geolevel, hele_jaren=False):
    """
    Transformeert de CBS data door filters toe te passen,
    berekeningen uit te voeren, en de dataset te herstructureren.

    Parameters:
    -----------
    df (pd.DataFrame): De originele dataset met CBS data.
    geolevel (str): Geografisch niveau (bijv. 'corop_id').
    hele_jaren (bool): Indien True, filter op hele jaren (standaard False).

    Returns:
    --------
    df (pd.DataFrame): De getransformeerde dataset.
    """
    # Selecteer alleen de relevante kolommen
    vereiste_kolommen = ['RegioS', 'Perioden', 'BevolkingAanHetEindeVanDePeriode_15']
    if not all(col in df.columns for col in vereiste_kolommen):
        raise KeyError("Een of meer vereiste kolommen ontbreken in de dataset.")
    df = df[vereiste_kolommen].copy()

    # FILTER: Alleen hele jaren als hele_jaren==True
    if hele_jaren:
        # Voorbeelden hele jaren: '2023', '2024JJ00'
        jaar_regex = re.compile(r"^(?:\d{4}JJ00|\d{4})$")
        df = df[df['Perioden'].astype(str).str.match(jaar_regex)]

    # Oppervlakte blijft stabiel, dus we kunnen deze gebruiken om de bevolkingsdichtheid te berekenen
    # Bron: 70072NED (2025JJ00)
    regio_oppervlakte_mapping = {
        'nl00': 41543.37,  # Nederland
        'cr37': 854.14,   # Noord-Limburg
        'cr38': 694.8,   # Midden-Limburg
        'cr39': 660.91    # Zuid-Limburg
    }

    df['mo_12d'] = df.apply(lambda row: (regio_oppervlakte_mapping.get(row['RegioS']) / row['BevolkingAanHetEindeVanDePeriode_15']) * 1000, axis=1)

    # Verwijder de nu overbodige kolommen
    df = df.drop(['BevolkingAanHetEindeVanDePeriode_15'], axis=1)

    # Verwijder rijen met missende waarden (indien aanwezig)
    df = df.dropna()

    # Hernoem kolommen voor leesbaarheid
    df = df.rename(columns={
        'RegioS': 'geoitem',
        'Perioden': 'period'
    })

    # Voeg een nieuwe kolom toe voor geolevel ('cr' voor COROP-regio's)
    df['geolevel'] = df['geoitem'].apply(lambda x: 'nederland' if x == 'nl00' else geolevel)

    return df




def transformeer_planrealisaties_juno(brond_bestand: pd.DataFrame, year: int, regio_mapping: dict) -> pd.DataFrame:
    """
    Transformeert planrealisaties data specifiek voor het Juno-bestand.

    Parameters:
    -----------
    brond_bestand (pd.DataFrame): DataFrame met de ruwe data van het Juno-bestand.
    year (int): Het jaar van de data.
    regio_mapping (dict): Mapping van COROP-namen naar geoitems.

    Returns:
    --------
    pd.DataFrame: Getransformeerde planrealisaties data.
    """
    # Inlezen van het Juno-bestand en selecteren van relevante kolommen
    df = pd.read_excel(brond_bestand)
    df = df[['Gemeente', 'COROP', 'Soort', 'Aantal toevoegingen', 'Aantal onttrekkingen', 'Huur/Koop', 'Prijsklasse', 'Woningtype', 'In-/uitbreidingslocatie']]

    # Toevoegen van geolevel en geoitem op basis van COROP-code
    df['geolevel'] = 'corop_code'
    df = df['COROP'].map(regio_mapping).to_frame('geoitem').join(df.drop(columns=['COROP']))
    df['period'] = year

    # Bepalen van d_40a (bruto toevoegingen) en d_40b (netto toevoegingen)
    df_d_40a = df.groupby(['geoitem', 'geolevel', 'period'])['Aantal toevoegingen'].sum().reset_index()
    df_d_40a = df_d_40a.rename(columns={'Aantal toevoegingen': 'd_40a'})

    # Netto toevoegingen berekenen als bruto toevoegingen minus onttrekkingen
    df_d_40b = df.groupby(['geoitem', 'geolevel', 'period'])['Aantal toevoegingen'].sum().reset_index()
    df_d_40b['d_40b'] = df_d_40b['Aantal toevoegingen'] - df.groupby(['geoitem', 'geolevel', 'period'])['Aantal onttrekkingen'].sum().reset_index()['Aantal onttrekkingen']
    df_d_40b = df_d_40b.drop(columns=['Aantal toevoegingen'])

    # 41a: Aandeel gerealiseerde woningen in de betaalbare prijsklasse t.o.v. het totale aantal gerealiseerde woningen.
    betaalbare_prijsklasse = [
        'Goedkope sociale huur', 'Betaalbare sociale huur', 'Dure sociale huur', 'Middeldure huur',
        'Betaalbaar laag koop', 'Betaalbaar midden koop', 'Betaalbaar hoog koop'
        ]
    
    # Maak een nieuwe kolom 'is_betaalbaar' die aangeeft of de woning in een betaalbare prijsklasse valt
    df['is_betaalbaar'] = df['Prijsklasse'].isin(betaalbare_prijsklasse)

    # df_d_41a berekenen door het aantal toevoegingen in betaalbare prijsklassen te sommeren en dit te delen door het totale aantal toevoegingen (d_40a)
    df_d_41a = df[df['is_betaalbaar']].groupby(['geoitem', 'geolevel', 'period'])['Aantal toevoegingen'].sum().reset_index()
    df_d_41a = df_d_41a.rename(columns={'Aantal toevoegingen': 'aantal_betaalbare_woningen'})
    df_d_41a = df_d_41a.merge(df_d_40a, on=['geoitem', 'geolevel', 'period'])

    # Bereken het aandeel betaalbare woningen ten opzichte van het totale aantal gerealiseerde woningen
    df_d_41a['d_41a'] = df_d_41a['aantal_betaalbare_woningen'] / df_d_41a['d_40a']
    df_d_41a = df_d_41a.drop(columns=['aantal_betaalbare_woningen', 'd_40a'])
    df_d_41a['d_41a'] = df_d_41a['d_41a'] * 100 # Omzetten naar percentage
    df_d_41a['d_41a'] = df_d_41a['d_41a'].apply(lambda x: f"{x:.1f}".replace('.', ','))

    return df_d_40a, df_d_40b, df_d_41a



def transformeer_planrealisaties(bron_bestanden: Union[str, List[str]], regio_mapping: dict) -> pd.DataFrame:
    """
    Laadt en transformeert planrealisaties data uit één of meerdere bronbestanden.

    Parameters:
    -----------
    bron_bestanden (str of list van str): Pad of paden naar de bronbestanden.
    regio_mapping (dict): Mapping van COROP-namen naar geoitems.

    Returns:
    --------
    pd.DataFrame: Getransformeerde planrealisaties data.
    """
    # Laad en verwerk de invoerbestanden
    df_2004_2023 = pd.read_excel(bron_bestanden[0])
    
    # Hernoemen van kolommen voor consistentie
    df = df_2004_2023.rename(columns={
        'periodcode': 'period',
        'geolevelcode': 'geolevel',
        'geoitemcode': 'geoitem'
    })

    # Selecteer en hernoem de relevante kolommen voor d_40a, d_40b en d_41a
    df_d_40a = df[df['variablecode'] == 'd_40a'][['geoitem', 'geolevel', 'period', 'value']].rename(columns={'value': 'd_40a'})
    df_d_40b = df[df['variablecode'] == 'd_40b'][['geoitem', 'geolevel', 'period', 'value']].rename(columns={'value': 'd_40b'})
    df_d_41a = df[df['variablecode'] == 'd_41a'][['geoitem', 'geolevel', 'period', 'value']].rename(columns={'value': 'd_41a'})

    year = 2024
    for bestand in bron_bestanden[1:]:
        temp_df_d_40a, temp_df_d_40b, temp_df_d_41a = transformeer_planrealisaties_juno(bestand, year, regio_mapping)
        
        # sorteer op geoitem, period
        df_d_40a = pd.concat([df_d_40a, temp_df_d_40a], ignore_index=True).sort_values(by=['geoitem', 'period'])
        df_d_40b = pd.concat([df_d_40b, temp_df_d_40b], ignore_index=True).sort_values(by=['geoitem', 'period'])
        df_d_41a = pd.concat([df_d_41a, temp_df_d_41a], ignore_index=True).sort_values(by=['geoitem', 'period'])

        year = year + 1

    return df_d_40a, df_d_40b, df_d_41a
    


