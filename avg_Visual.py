import pandas as pd
import matplotlib.pyplot as plt
import json
from datetime import datetime, timedelta
import matplotlib.dates as mdates
import numpy as np
import os

class PoolDataVisualizer:
    def __init__(self, csv_file):
        """Initialisiert den Visualizer mit der CSV-Datei"""
        self.csv_file = csv_file
        self.df = None
        self.parsed_pools = {}

    def load_data(self):
        """Lädt die CSV-Daten"""
        try:
            print(f"🔄 Lade Daten aus {self.csv_file}...")
            self.df = pd.read_csv(self.csv_file)
            print(f"✅ {len(self.df)} Datensätze erfolgreich geladen")
            return True
        except FileNotFoundError:
            print(f"❌ Datei {self.csv_file} nicht gefunden!")
            return False
        except Exception as e:
            print(f"❌ Fehler beim Laden der Datei: {e}")
            return False

    def parse_pool_data(self):
        """Parsed die avgpool JSON-Daten in separate Spalten"""
        print("🔄 Parse Pool-Daten...")

        all_keys = set()
        valid_pools = []

        for idx, row in self.df.iterrows():
            try:
                pool_data = json.loads(row['avgpool'].replace("'", '"'))
                valid_pools.append(pool_data)

                for item in pool_data:
                    all_keys.add(item['key'])

            except Exception as e:
                print(f"⚠️ Fehler beim Parsen von Zeile {idx}: {e}")
                valid_pools.append([])

        for key in all_keys:
            self.df[f'pool_{key}'] = 0.0

        for idx, pool_data in enumerate(valid_pools):
            if pool_data: 
                for item in pool_data:
                    key = item['key']
                    value = float(item['value'].replace(' TLM', ''))
                    self.df.at[idx, f'pool_{key}'] = value

        self.df['day'] = pd.to_datetime(self.df['day'])

        print(f"✅ Pool-Daten erfolgreich geparst")
        print(f"📊 Verfügbare Pool-Typen: {sorted(all_keys)}")

        self.parsed_pools = {key: f'pool_{key}' for key in all_keys}

    def create_bar_chart(self, pool_type, days=None, save_dir=None):
        """Erstellt Balkendiagramm für einen bestimmten Pool-Typ und Zeitraum und speichert es optional"""
        if pool_type not in self.parsed_pools:
            print(f"❌ Pool-Typ '{pool_type}' nicht gefunden!")
            return

        col_name = self.parsed_pools[pool_type]

        # Daten filtern basierend auf dem Zeitraum
        chart_data = self.df[['day', col_name, 'numberofentries']].copy()
        
        if days is not None:
            # Filtere die Daten auf die letzten X Tage
            latest_date = chart_data['day'].max()
            cutoff_date = latest_date - timedelta(days=days)
            chart_data = chart_data[chart_data['day'] >= cutoff_date]
            time_period = f" (Last {days} days)"
            filename_suffix = f"_{days}days"
        else:
            time_period = ""
            filename_suffix = ""

        chart_data = chart_data.sort_values('day')

        if len(chart_data) == 0:
            print(f"⚠️ Keine Daten für Pool '{pool_type}' im angegebenen Zeitraum")
            return

        avg_value = chart_data[col_name].mean()
        max_value = chart_data[col_name].max()
        min_value = chart_data[col_name].min()
        total_entries = chart_data['numberofentries'].sum()

        plt.figure(figsize=(15, 8))

        bars = plt.bar(chart_data['day'], chart_data[col_name], 
                      color='steelblue', alpha=0.7, edgecolor='navy', linewidth=0.5)

        plt.title(f'Pool: {pool_type}{time_period}\n'
                 f'Average: {avg_value:.4f} TLM | Max: {max_value:.4f} TLM | Min: {min_value:.4f} TLM',
                 fontsize=16, fontweight='bold', pad=20)

        plt.xlabel('Date', fontsize=12, fontweight='bold')
        plt.ylabel(f'{pool_type} Pool (TLM)', fontsize=12, fontweight='bold')

        ax = plt.gca()

        date_range = (chart_data['day'].max() - chart_data['day'].min()).days

        if date_range <= 7 or days == 7:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%a\n%d.%m.%Y'))
            ax.xaxis.set_major_locator(mdates.DayLocator())
        elif date_range <= 31 or days == 30:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, date_range // 10)))
        else:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%Y'))
            ax.xaxis.set_major_locator(mdates.MonthLocator())

        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

        plt.grid(True, alpha=0.3, linestyle='--')

        plt.tight_layout()

        info_text = f'Anzahl Tage: {len(chart_data)} | Gesamt Einträge: {total_entries}'
        plt.figtext(0.02, 0.02, info_text, fontsize=10, alpha=0.7)

        if save_dir:
            # Erstelle den Ordner, falls er nicht existiert
            os.makedirs(save_dir, exist_ok=True)
            # Erstelle den Dateinamen
            filename = f"pool_{pool_type}{filename_suffix}.png"
            filepath = os.path.join(save_dir, filename)
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            print(f"💾 Diagramm gespeichert als: {filepath}")
        else:
            plt.show()
        
        plt.close()

    def _create_all_pools_chart(self, save_dir=None):
        """Erstellt ein kombiniertes Diagramm für alle Pool-Typen und speichert es optional"""
        pool_types = list(self.parsed_pools.keys())
        n_pools = len(pool_types)

        cols = 3
        rows = (n_pools + cols - 1) // cols

        plt.figure(figsize=(20, 5 * rows))
        plt.suptitle('All Pools', fontsize=20, fontweight='bold')

        for i, pool_type in enumerate(sorted(pool_types), 1):
            col_name = self.parsed_pools[pool_type]

            plt.subplot(rows, cols, i)

            chart_data = self.df[['day', col_name]].copy()
            chart_data = chart_data.sort_values('day')

            plt.bar(chart_data['day'], chart_data[col_name], 
                   color=plt.cm.Set3(i), alpha=0.7)

            plt.title(f'{pool_type}\n(Ø {chart_data[col_name].mean():.4f} TLM)', 
                     fontweight='bold')
            plt.xlabel('Date')
            plt.ylabel('TLM')

            # X-Achse formatieren
            ax = plt.gca()
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
            plt.grid(True, alpha=0.3)

        plt.tight_layout()
        
        if save_dir:
            # Erstelle den Ordner, falls er nicht existiert
            os.makedirs(save_dir, exist_ok=True)
            filepath = os.path.join(save_dir, "all_pools.png")
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            print(f"💾 Kombiniertes Diagramm gespeichert als: {filepath}")
        else:
            plt.show()
        
        plt.close()

    def run_sequential(self, save_dir="pool_plots"):
        """Startet den sequentiellen Modus - speichert alle Pools als Bilder"""
        if not self.load_data():
            return

        self.parse_pool_data()

        print("\n" + "="*60)
        print("🎯 POOL-DATEN VISUALIZER (AUTOSAVE MODUS)")
        print("="*60)

        print(f"\n📊 Erstelle kombiniertes Diagramm für alle Pools...")
        self._create_all_pools_chart(save_dir)

        pool_types = sorted(self.parsed_pools.keys())
        
        print(f"\n📊 Erstelle {len(pool_types)} einzelne Pool-Diagramme...")
        
        for i, pool_type in enumerate(pool_types, 1):
            print(f"📊 Erstelle Diagramm {i}/{len(pool_types)}: {pool_type}")
            
            # Vollständigen Zeitraum
            self.create_bar_chart(pool_type, save_dir=save_dir)
            
            # Letzte 30 Tage
            self.create_bar_chart(pool_type, days=30, save_dir=save_dir)
            
            # Letzte 7 Tage
            self.create_bar_chart(pool_type, days=7, save_dir=save_dir)
            
        print(f"\n✅ Alle {len(pool_types)} Pool-Diagramme wurden gespeichert!")
        print(f"📁 Bilder gespeichert in: {save_dir}")
        print("👋 Auf Wiedersehen!")

def main():
    INPUT_FILE = "avgpooldata.csv"
    OUTPUT_DIR = "pool_plots"  # Ordner für die gespeicherten Bilder

    visualizer = PoolDataVisualizer(INPUT_FILE)
    visualizer.run_sequential(OUTPUT_DIR)

if __name__ == "__main__":
    main()