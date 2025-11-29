import pandas as pd

# Geocode cache'i oku
geocode = pd.read_csv('data/geocode_cache.csv')
print(f"Geocode cache: {len(geocode)} istasyon")

# Temizlenmiş veriyi oku
trips = pd.read_csv('data/2021-04_cleaned.csv')
print(f"Trips data: {len(trips)} yolculuk")

# Departure (başlangıç) istasyonlarının konum bilgilerini ekle
trips = trips.merge(
    geocode[['station_id', 'lat', 'lon']],
    left_on='Departure station id',
    right_on='station_id',
    how='left'
).rename(columns={'lat': 'departure_lat', 'lon': 'departure_lon'}).drop('station_id', axis=1)

# Return (bitiş) istasyonlarının konum bilgilerini ekle
trips = trips.merge(
    geocode[['station_id', 'lat', 'lon']],
    left_on='Return station id',
    right_on='station_id',
    how='left'
).rename(columns={'lat': 'return_lat', 'lon': 'return_lon'}).drop('station_id', axis=1)

# Birleştirilmiş veriyi kaydet
trips.to_csv('data/2021-04_merged.csv', index=False)
print(f"\nBirleştirilmiş veri kaydedildi: data/2021-04_merged.csv")
print(f"Kolonlar: {list(trips.columns)}")
print(f"\nİlk birkaç satır:")
print(trips.head())

# Eksik konum bilgisi olan kayıtları kontrol et
missing_departure = trips['departure_lat'].isna().sum()
missing_return = trips['return_lat'].isna().sum()
print(f"\nEksik departure konum bilgisi: {missing_departure}")
print(f"Eksik return konum bilgisi: {missing_return}")
