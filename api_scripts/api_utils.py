import cbsodata
import pandas as pd

# Tijdelijke oplossing voor het definiëren van Limburgse COROP-regio's
limburg_dict = {
    'nederland': 'nl00',
    'cr': {
        'Noord-Limburg': 'cr37',
        'Midden-Limburg': 'cr38',
        'Zuid-Limburg': 'cr39'
    }
}

def download_cbs_data(table_code, geolevel=None, filter_limburg=False, convert_to_geolevel_codes=False, keep_nl_data=True):
    """
    Haalt een CBS-tabel op aan de hand van een opgegeven tabelcode, 
    en kan optioneel filteren op een specifiek geolevel (bijv. LD, PV, CR),
    en/of specifiek Limburgse COROP-regio's en regio-namen vertalen naar corresponderende codes.
    Parameters:
    -----------
    table_code (str): De tabelcode van de CBS-dataset (bijv. '70072NED').
    geolevel (str, optional): Het geolevel om op te filteren (bijv. 'LD', 'PV', 'CR').
    filter_limburg (bool, optional): Filter alleen Limburgse COROP-regio's indien `True`.
    convert_to_geolevel_codes (bool, optional): Converteer namen in 'RegioS' naar codes indien `True`.
    keep_nl_data (bool, optional): Houd de rijen waar 'RegionS == Nederland' ongeacht de toegepaste filters.
    Returns:
    --------
    pd.DataFrame: De volledige of gefilterde dataset.
    """
    # Download de volledige tabel
    data = pd.DataFrame(cbsodata.get_data(table_code))
    print(f"Dataset met tabelcode '{table_code}' succesvol opgehaald.")
    
    # Controleer of 'RegionS' bestaat
    if 'RegioS' not in data.columns:
        raise KeyError("De kolom 'RegioS' ontbreekt in de dataset.")
    
    # Opslaan van Nederland-rijen (indien vereist)
    nl_data = pd.DataFrame()
    if keep_nl_data:
        nl_data = data[data['RegioS'].str.lower() == 'nederland'].copy()
        print(f"Nederland-rijen apart opgeslagen: {len(nl_data)} rijen.")
        if convert_to_geolevel_codes:
            # Map 'nederland' naar 'nl00'
            nl_data['RegioS'] = nl_data['RegioS'].apply(lambda x: 'nl00' if x.lower().strip() == 'nederland' else x)
    
    # Opschonen en geolevel-suffix verwijderen uit 'RegioS'
    data['RegioS'] = (
        data['RegioS']  # Original column
        .str.lower()  # Lowercase for case insensitivity
        .str.replace("-", " ")  # Replace hyphens with spaces
        .str.strip()  # Remove whitespace
    )
    print(f"RegioS opgeschoond.")
    
    # Filter op geolevel indien opgegeven
    if geolevel:
        geolevel = geolevel.lower()  # Ensure lowercase matching
        data = data[data['RegioS'].str.endswith(f"({geolevel})")]
        print(f"Data gefilterd op geolevel '{geolevel}': {len(data)} rijen over.")
        # Remove geolevel suffix
        data['RegioS'] = data['RegioS'].str.replace(f"({geolevel})", "", regex=False).str.strip()
        print(f"Geolevel-suffix '({geolevel})' verwijderd.")
    
    # Filter specifiek Limburgse COROP-regio's indien aangevraagd
    if filter_limburg:
        if geolevel != 'cr':
            raise ValueError("Limburg-filter kan alleen worden toegepast op geolevel 'CR'.")
        # Haal de Limburg-namen op uit de limburg_dict
        limburg_codes = [val.lower().replace("-", " ").strip() for val in limburg_dict[geolevel].keys()]
        # Filter Limburgse COROP-regio's
        data = data[data['RegioS'].str.contains('|'.join(limburg_codes))]
        print(f"Data gefilterd op Limburgse COROP-regio's: {len(data)} rijen over.")
    
    # Converteer regio-namen in 'RegioS' naar hun corresponderende geolevel codes
    if convert_to_geolevel_codes:
        if geolevel in limburg_dict:
            name_to_code = {name.lower().replace("-", " ").strip(): code for name, code in limburg_dict[geolevel].items()}
            # Map names to codes
            data['RegioS'] = data['RegioS'].map(name_to_code).fillna(data['RegioS'])
            print("RegioS-namen vertaald naar codes.")
    
    # Voeg Nederland-rijen terug indien vereist
    if keep_nl_data and len(nl_data) > 0:
        data = pd.concat([data, nl_data], ignore_index=True)
        print(f"Nederland-rijen toegevoegd aan de dataset: totaal {len(data)} rijen.")
    
    return data