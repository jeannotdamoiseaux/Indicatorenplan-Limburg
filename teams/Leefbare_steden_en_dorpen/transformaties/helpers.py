import pandas as pd
import warnings

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

def laad_en_verwerk_enkel_invoerbestand(bron_bestand: str) -> pd.DataFrame:
    """
    Laadt en verwerkt één enkel bestand volgens de gewenste structuur.
    """
    # Bestand inlezen afhankelijk van extensie
    try:
        if bron_bestand.endswith(".csv"):
            df = pd.read_csv(bron_bestand, sep=';')
        elif bron_bestand.endswith(".xlsx") or bron_bestand.endswith(".xls"):
            df = pd.read_excel(bron_bestand)
        else:
            raise ValueError(f"Bestandstype niet ondersteund: {bron_bestand}")
    except FileNotFoundError:
        raise FileNotFoundError(f"Het bestand '{bron_bestand}' is niet gevonden. Controleer het pad en bestand.")
    except Exception as e:
        raise RuntimeError(f"Er is een fout opgetreden tijdens het inlezen van het bestand: {e}")
    
    print(f"Bestand '{bron_bestand}' succesvol ingelezen. Verwerken van data...")

    # Kolom 'Operationeel databewaker' checken & evt. verwijderen
    if "Operationeel databewaker" in df.columns:
        warnings.warn(
            f"De kolom 'Operationeel databewaker' is nog aanwezig in het bestand: {bron_bestand} "
            "Verwijder deze kolom voordat je publiceert naar GitHub!",
            UserWarning
        )
        df = df.drop(columns=["Operationeel databewaker"])

    df['geolevel'] = 'prov_id'
    df['geoitem'] = 'pv31'

    # Hernoemen 'Datum van invoer' naar 'period'
    if "Datum van invoer" in df.columns:
        print(f"'Datum van invoer' kolom gevonden in bestand: {bron_bestand}, deze wordt hernoemd naar 'period'.")
        df.rename(columns={'Datum van invoer': 'period'}, inplace=True)

    # Gebruik 'Peildatum (indien afwijkend van datum van invoer)' indien ingevuld
    if "Peildatum (indien afwijkend van datum van invoer)" in df.columns:
        print(f"'Peildatum (indien afwijkend van datum van invoer)' kolom gevonden in bestand: {bron_bestand}, deze zal worden gebruikt als 'period' indien deze niet leeg is.")
        df['period'] = df.apply(
            lambda row: row['Peildatum (indien afwijkend van datum van invoer)'] 
            if pd.notnull(row['Peildatum (indien afwijkend van datum van invoer)']) 
            else row['period'], 
            axis=1
        )
    
    # Controleer de kolommen in het DataFrame
    verwachte_kolommen = {'geolevel', 'geoitem', 'period', 'Indicator_nr', 'Invoerveld'}
    if not verwachte_kolommen.issubset(df.columns):
        ontbrekende_kolommen = verwachte_kolommen - set(df.columns)
    
        # print de kolommen die aanwezig zijn in het DataFrame voor debugging
        print(f"Kolommen aanwezig in het DataFrame van bestand '{bron_bestand}': {', '.join(df.columns)}")

        raise KeyError(f"Het bestand '{bron_bestand}' mist de volgende vereiste kolommen: {', '.join(ontbrekende_kolommen)}")

    # Datum verwerken, eventueel converteren naar gewenst stringformaat, met controle
    try:
        df['period'] = pd.to_datetime(df['period'], format='%d-%m-%Y', errors='coerce')
    except Exception as e:
        raise RuntimeError(f"Er is een fout opgetreden bij het verwerken van de dates in de 'period'-kolom in bestand: {bron_bestand}, fout: {e}")

    df['period'] = df['period'].apply(lambda x: f"m{x.month}y{x.year}" if not pd.isna(x) else None)

    # Pivot zodat Indicator_nr kolommen worden en Invoerveld de waarden
    df_pivot = df.pivot_table(
        index=['geolevel', 'geoitem', 'period'],
        columns='Indicator_nr',
        values='Invoerveld',
        aggfunc='first'
    ).reset_index()

    df_pivot.columns.name = None
    df_pivot = df_pivot.rename_axis(None, axis=1)
    return df_pivot