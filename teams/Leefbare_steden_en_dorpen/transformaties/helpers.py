import pandas as pd

def drop_empty_rows(df):
    """
    Verwijdert volledig lege rijen uit het DataFrame en reset de index.

    Args:
        df (pd.DataFrame): Het originele DataFrame.

    Returns:
        pd.DataFrame: DataFrame zonder lege rijen en met gereset index.
    """
    # Reset de index na het verwijderen van lege rijen om gaten te voorkomen
    return df.reset_index(drop=True)

def create_column_mapping(header_row):
    """
    Creëert een mapping voor kolomnamen op basis van de eerste rij.

    Args:
        header_row (pd.Series): De eerste rij van het DataFrame.

    Returns:
        list: Een lijst met nieuwe kolomnamen.
    """
    new_column_names = []
    current_name = None
    # Doorloop de kolomnamen in omgekeerde volgorde
    for col_name in reversed(header_row.values):
        if pd.notna(col_name):
            current_name = col_name
        new_column_names.append(current_name)
    return new_column_names

def extract_regios_and_jaartallen(df, n_rows):
    """
    Extraheert unieke regio's en jaartallen uit het DataFrame.

    Args:
        df (pd.DataFrame): Het DataFrame met regio's en jaartallen.
        n_rows (int): Aantal rijen dat relevante informatie bevat.

    Returns:
        tuple: Een set van unieke regio's en een set van unieke jaartallen.
    """
    regios = set(df['Regio'].iloc[:n_rows].dropna())  # Unieke regio's
    jaartallen = set(df.iloc[1, :].dropna().unique())  # Unieke jaartallen
    return regios, jaartallen

def process_data(df, regios, jaartallen):
    """
    Verwerkt de data in een nieuw formaat met 'Regio', 'Jaartal', 'Categorie' en 'Waarde'.

    Args:
        df (pd.DataFrame): Het originele DataFrame.
        regios (set): Set van unieke regio's.
        jaartallen (set): Set van unieke jaartallen.

    Returns:
        pd.DataFrame: Verwerkt DataFrame met kolommen 'Regio', 'Jaartal', 'Categorie' en 'Waarde'.
    """
    result_df = pd.DataFrame()
    for regio in regios:
        for jaartal in jaartallen:
            # Filter kolommen op basis van jaartal
            filtered_cols = df.iloc[1, :] == jaartal
            filtered_cols.loc['Regio'] = True  # Zorg ervoor dat 'Regio' altijd meegenomen wordt
            
            # Filter rijen op basis van regio
            result = df.loc[:, filtered_cols]
            result = result[result['Regio'] == regio]
            
            # Smelt het DataFrame
            df_melted = result.melt(id_vars=["Regio"], 
                                    var_name="Categorie", 
                                    value_name="Waarde")
            df_melted['Jaartal'] = int(jaartal)
            
            # Voeg de gesmolten data toe aan het resultaat
            result_df = pd.concat([result_df, df_melted], ignore_index=True)
    
    # Verwerk de waarden: vervang '.' door '0' en converteer naar numeriek
    result_df['Waarde'] = result_df['Waarde'].replace(".", "0")
    result_df['Waarde'] = pd.to_numeric(result_df['Waarde'], errors='coerce')
    
    return result_df

def corrigeer_bu_codes(df, bu_code_kolom, pad_naar_bu_code_correcties):
    """
    Corrigeert buurtcodes in een DataFrame op basis van een correctiebestand.

    Deze functie voert de volgende stappen uit:
    1. Leest een Excel-bestand met buurtcode correcties
    2. Zet alle buurtcodes om naar hoofdletters voor consistentie
    3. Past de correcties toe op de gespecificeerde kolom in het DataFrame

    Args:
        df (pd.DataFrame): Het DataFrame waarin de buurtcodes gecorrigeerd moeten worden
        bu_code_kolom (str): De naam van de kolom in het DataFrame die de buurtcodes bevat
        pad_naar_bu_code_correcties (str): Het pad naar het Excel-bestand met de buurtcode correcties

    Returns:
        pd.DataFrame: Het DataFrame met gecorrigeerde buurtcodes

    Raises:
        FileNotFoundError: Als het correctiebestand niet gevonden kan worden
        KeyError: Als de gespecificeerde bu_code_kolom niet in het DataFrame bestaat

    Note:
        Het correctiebestand moet een Excel-bestand zijn met ten minste twee kolommen:
        'BUURT_CODE' (de originele buurtcodes) en 'BUURT_CODE_CORRECTIE' (de gecorrigeerde buurtcodes)
    """
    # Lees het bestand met de buurtcode correcties
    try:
        correctie_df = pd.read_excel(pad_naar_bu_code_correcties)
        correctie_dict = correctie_df.set_index('BUURT_CODE')['BUURT_CODE_CORRECTIE'].to_dict()
    except FileNotFoundError:
        raise FileNotFoundError(f"Het correctiebestand kon niet worden gevonden op het pad: {pad_naar_bu_code_correcties}")
    
    # Zet alle waardes om naar hoofdletters voor consistentie
    correctie_dict = {key.upper(): value.upper() for key, value in correctie_dict.items()}
    
    # Pas de correcties toe op de buurtcodes in het DataFrame
    try:
        df[bu_code_kolom] = df[bu_code_kolom].apply(lambda x: correctie_dict.get(x.upper(), x.upper()))
    except KeyError:
        raise KeyError(f"De kolom '{bu_code_kolom}' bestaat niet in het DataFrame")
    
    return df

def filter_limburgse_buurten(df, buurt_code_kolom, pad_naar_limburgse_buurten):
    """
    Filtert een DataFrame om alleen Limburgse buurten te behouden op basis van een Excel-bestand met Limburgse buurtcodes.

    Deze functie voert de volgende stappen uit:
    1. Leest een Excel-bestand met Limburgse buurtcodes
    2. Controleert of de gespecificeerde buurtcode kolom in het DataFrame aanwezig is
    3. Filtert het DataFrame om alleen rijen te behouden waar de buurtcode voorkomt in de lijst van Limburgse buurtcodes
    4. Reset de index van het gefilterde DataFrame

    Args:
        df (pd.DataFrame): Het originele DataFrame met buurtcodes
        buurt_code_kolom (str): De naam van de kolom in het DataFrame die de buurtcodes bevat
        pad_naar_limburgse_buurten (str): Het pad naar het Excel-bestand met Limburgse buurtcodes

    Returns:
        pd.DataFrame: Een gefilterd DataFrame met alleen Limburgse buurten

    Raises:
        KeyError: Als de gespecificeerde buurt_code_kolom niet in het DataFrame bestaat
        FileNotFoundError: Als het Excel-bestand met Limburgse buurtcodes niet gevonden kan worden
        ValueError: Als er geen Limburgse buurten in het DataFrame worden gevonden na filtering

    Note:
        Zorg ervoor dat het Excel-bestand met Limburgse buurtcodes een kolom 'BU_CODE' bevat met de buurtcodes.
    """
    # Lees het Excel-bestand met Limburgse buurtcodes
    try:
        bu_codes_limburg = pd.read_excel(pad_naar_limburgse_buurten)['BU_CODE'].tolist()
    except FileNotFoundError:
        raise FileNotFoundError(f"Het bestand met Limburgse buurtcodes kon niet worden gevonden op het pad: {pad_naar_limburgse_buurten}")
    except KeyError:
        raise KeyError("Het Excel-bestand moet een kolom 'BU_CODE' bevatten met de Limburgse buurtcodes")

    # Controleer of de gespecificeerde kolom in het DataFrame aanwezig is
    if buurt_code_kolom not in df.columns:
        raise KeyError(f"De kolom '{buurt_code_kolom}' bestaat niet in het DataFrame")

    # Filter het DataFrame om alleen Limburgse buurten te behouden
    df_limburg = df[df[buurt_code_kolom].isin(bu_codes_limburg)]

    # Controleer of er Limburgse buurten zijn gevonden
    if df_limburg.empty:
        raise ValueError("Er zijn geen Limburgse buurten gevonden in het DataFrame")

    # Reset de index van het gefilterde DataFrame
    df_limburg = df_limburg.reset_index(drop=True)

    return df_limburg

def map_bu_code_naar_corop_code(df, buurt_code_kolom, pad_naar_limburgse_buurten):
    """
    Voegt een COROP_NAAM kolom toe aan het DataFrame door de buurtcode te mappen naar de bijbehorende COROP naam.

    Deze functie voert de volgende stappen uit:
    1. Leest een Excel-bestand met buurtcodes en bijbehorende COROP namen
    2. Creëert een mapping dictionary van buurtcode naar COROP naam
    3. Voegt een nieuwe kolom 'COROP_NAAM' toe aan het DataFrame gebaseerd op de mapping

    Args:
        df (pd.DataFrame): Het originele DataFrame met buurtcodes
        buurt_code_kolom (str): De naam van de kolom in het DataFrame die de buurtcodes bevat
        pad_naar_limburgse_buurten (str): Het pad naar het Excel-bestand met buurtcodes en COROP namen

    Returns:
        pd.DataFrame: Het originele DataFrame met een extra 'COROP_NAAM' kolom

    Raises:
        FileNotFoundError: Als het Excel-bestand niet gevonden kan worden
        KeyError: Als de vereiste kolommen niet in het Excel-bestand of het DataFrame aanwezig zijn

    Note:
        Het Excel-bestand moet kolommen 'BU_CODE' en 'COROP_NAAM' bevatten.
    """
    try:
        bu_wk_gm_codes = pd.read_excel(pad_naar_limburgse_buurten)
    except FileNotFoundError:
        raise FileNotFoundError(f"Het bestand met Limburgse buurtcodes kon niet worden gevonden op het pad: {pad_naar_limburgse_buurten}")
    
    # Controleer of de vereiste kolommen aanwezig zijn in het Excel-bestand
    required_columns = ['BU_CODE', 'COROP_NAAM']
    if not all(col in bu_wk_gm_codes.columns for col in required_columns):
        raise KeyError(f"Het Excel-bestand moet de volgende kolommen bevatten: {', '.join(required_columns)}")
    
    # Controleer of de buurt_code_kolom aanwezig is in het DataFrame
    if buurt_code_kolom not in df.columns:
        raise KeyError(f"De kolom '{buurt_code_kolom}' bestaat niet in het DataFrame")
    
    # Creëer een mapping dictionary van bu_code naar corop_code
    mapping_dict = dict(zip(bu_wk_gm_codes['BU_CODE'], bu_wk_gm_codes['COROP_NAAM']))
    
    # Map de bu_code naar corop_code met behulp van de mapping dictionary
    df['COROP_NAAM'] = df[buurt_code_kolom].map(mapping_dict)
    
    return df