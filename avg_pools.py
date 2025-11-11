#!/usr/bin/env python3
"""
Improved CSV Pool Data Processor
Verarbeitet ALLE CSV-Daten mit Pool-Informationen und erstellt tägliche Zusammenfassungen
während die ursprünglichen Keys (Abundant, Common, Epic, Legendary, Mythical, Rare) erhalten bleiben
"""

import pandas as pd
import json
import ast
from datetime import datetime
from collections import defaultdict
import sys

def parse_pool_data(pool_str):
    """
    Parse pool data string und extrahiere key-value Paare

    Args:
        pool_str: String mit Pool-Daten im JSON-ähnlichen Format

    Returns:
        Dictionary mit Pool-Keys und ihren entsprechenden TLM-Werten
    """
    pool_data = {}

    try:
        # Versuche zuerst als valides JSON zu parsen
        if isinstance(pool_str, str) and pool_str.strip():
            # Entferne mögliche Anführungszeichen am Anfang und Ende
            clean_str = pool_str.strip('"').strip("'")

            # Versuche ast.literal_eval (sicherer als eval)
            pool_list = ast.literal_eval(clean_str)

            for item in pool_list:
                if isinstance(item, dict) and 'key' in item and 'value' in item:
                    # Extrahiere numerischen Wert aus Strings wie "0.1083 TLM"
                    value_str = str(item['value']).replace(' TLM', '').strip()
                    try:
                        pool_data[item['key']] = float(value_str)
                    except ValueError:
                        print(f"Warning: Could not convert value '{value_str}' to float for key '{item['key']}'")

    except (json.JSONDecodeError, ValueError, SyntaxError) as e:
        print(f"Warning: Could not parse pool data: {pool_str[:100]}... Error: {e}")

    return pool_data

def extract_date(date_str):
    """
    Extrahiere Datumsteil aus datetime string

    Args:
        date_str: Datum-String im Format "YYYY-MM-DD HH:MM:SS" oder ähnlich

    Returns:
        Datum-String im Format "YYYY-MM-DD"
    """
    try:
        if 'T' in str(date_str):
            date_part = str(date_str).split('T')[0]
        else:
            date_part = str(date_str).split(' ')[0]
        return date_part
    except:
        return str(date_str)

def calculate_daily_averages(daily_pools):
    """
    Berechne tägliche Durchschnitte für jeden Pool-Key

    Args:
        daily_pools: Dictionary mit Pool-Daten für einen Tag

    Returns:
        List von Dictionaries mit key-value Paaren für Durchschnittswerte
    """
    if not daily_pools:
        return []

    # Sammle alle Keys
    all_keys = set()
    for pool_data in daily_pools:
        all_keys.update(pool_data.keys())

    # Berechne Durchschnitt für jeden Key
    avg_pools = []
    for key in sorted(all_keys):  # Sortiert für konsistente Reihenfolge
        values = [pool_data.get(key, 0) for pool_data in daily_pools if key in pool_data]
        if values:
            avg_value = sum(values) / len(values)
            avg_pools.append({
                'key': key,
                'value': f'{avg_value:.4f} TLM'
            })

    return avg_pools

def process_csv_data(input_file, output_file):
    """
    Verarbeite CSV-Daten und erstelle tägliche Zusammenfassung mit erhaltenen Keys

    Args:
        input_file: Pfad zur Eingabe-CSV-Datei
        output_file: Pfad zur Ausgabe-CSV-Datei
    """

    try:
        # Lese die CSV-Datei - OHNE LIMIT!
        print(f"Lade Daten aus {input_file}...")

        # Verwende pandas.read_csv OHNE nrows Parameter um ALLE Zeilen zu lesen
        df = pd.read_csv(input_file)

        total_records = len(df)
        print(f"✓ Erfolgreich {total_records} Datensätze geladen")

        if total_records == 0:
            print("Fehler: Keine Daten in der Datei gefunden!")
            return

        # Extrahiere Datum (nur Tag) aus der Datumsspalte
        print("Extrahiere Datumsangaben...")
        df['day'] = df['date'].apply(extract_date)

        # Parse Pool-Daten für jede Zeile
        print(f"Parse Pool-Daten für alle {total_records} Datensätze...")
        df['parsed_pool'] = df['pool'].apply(parse_pool_data)

        # Entferne Zeilen wo Pool-Parsing fehlgeschlagen ist
        valid_rows = df[df['parsed_pool'].apply(lambda x: len(x) > 0)]
        invalid_count = total_records - len(valid_rows)

        if invalid_count > 0:
            print(f"⚠ {invalid_count} Zeilen konnten nicht geparst werden und wurden übersprungen")

        print(f"✓ {len(valid_rows)} Datensätze erfolgreich verarbeitet")

        # Gruppiere nach Tag
        print("Gruppiere Daten nach Tagen...")
        daily_groups = valid_rows.groupby('day')

        print(f"✓ Daten für {len(daily_groups)} verschiedene Tage gefunden")

        # Erstelle tägliche Zusammenfassung
        daily_summary = []
        day_id = 1

        for day, group in daily_groups:
            print(f"Verarbeite Tag {day} mit {len(group)} Einträgen...")

            # Sammle alle Pool-Daten für diesen Tag
            daily_pools = group['parsed_pool'].tolist()

            # Berechne Durchschnitte
            avg_pools = calculate_daily_averages(daily_pools)

            # Konvertiere zu JSON-String für CSV
            avg_pools_json = json.dumps(avg_pools, ensure_ascii=False)

            daily_summary.append({
                'id': day_id,
                'day': day,
                'avgpool': avg_pools_json,
                'numberofentries': len(group)
            })

            day_id += 1

        # Erstelle DataFrame und speichere als CSV
        summary_df = pd.DataFrame(daily_summary)
        summary_df.to_csv(output_file, index=False, encoding='utf-8')

        print(f"\n✓ Erfolgreich abgeschlossen!")
        print(f"✓ Eingabe: {total_records} Datensätze verarbeitet")
        print(f"✓ Ausgabe: {len(daily_summary)} Tage zusammengefasst")
        print(f"✓ Ergebnis gespeichert in: {output_file}")

        # Zeige Statistiken
        print("\n=== STATISTIKEN ===")
        for day_info in daily_summary:
            print(f"Tag {day_info['day']}: {day_info['numberofentries']} Einträge")

        return summary_df

    except FileNotFoundError:
        print(f"Fehler: Datei '{input_file}' nicht gefunden!")
        return None
    except Exception as e:
        print(f"Fehler beim Verarbeiten der Daten: {e}")
        print("Stacktrace:")
        import traceback
        traceback.print_exc()
        return None

def main():
    """
    Hauptfunktion - Hier können Sie Ihre Dateipfade anpassen
    """

    # TODO: Passen Sie diese Pfade an Ihre tatsächlichen Dateien an!
    INPUT_FILE = "minepooldata.csv"  # Ändern Sie dies zu Ihrem tatsächlichen Dateipfad
    OUTPUT_FILE = "avgpooldata.csv"

    print("=== CSV Pool Data Processor (Vollversion) ===")
    print(f"Eingabedatei: {INPUT_FILE}")
    print(f"Ausgabedatei: {OUTPUT_FILE}")
    print()

    # Verarbeite die Daten
    result = process_csv_data(INPUT_FILE, OUTPUT_FILE)

    if result is not None:
        print("\n=== ERFOLGREICH ABGESCHLOSSEN ===")
        print("Ihre tägliche Zusammenfassung wurde erstellt!")
    else:
        print("\n=== FEHLER ===")
        print("Die Verarbeitung ist fehlgeschlagen.")

if __name__ == "__main__":
    main()
