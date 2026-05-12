import pandas as pd

putanja = r"C:\SRP_projekt-main\checkpoint2\hotel_bookings2.csv"
df = pd.read_csv(putanja)

region_countries = {
    "Southern Europe": ["Albania", "Andorra", "Bosnia and Herzegovina", "Croatia", "Cyprus", "Gibraltar", "Greece", "Italy", "Malta", "Montenegro", "North Macedonia", "Portugal", "San Marino", "Serbia", "Slovenia", "Spain"],
    "Northern Europe": ["Denmark", "Estonia", "Faroe Islands", "Finland", "Guernsey", "Iceland", "Ireland", "Isle of Man", "Jersey", "Latvia", "Lithuania", "Norway", "Sweden", "United Kingdom"],
    "Western Europe": ["Austria", "Belgium", "France", "Germany", "Liechtenstein", "Luxembourg", "Monaco", "Netherlands", "Switzerland"],
    "Eastern Europe": ["Belarus", "Bulgaria", "Czechia", "Hungary", "Poland", "Romania", "Russian Federation", "Slovakia", "Ukraine"],
    "Western Asia": ["Armenia", "Azerbaijan", "Bahrain", "Georgia", "Iraq", "Israel", "Jordan", "Kuwait", "Lebanon", "Oman", "Qatar", "Saudi Arabia", "Syrian Arab Republic", "Türkiye", "United Arab Emirates"],
    "Southern Asia": ["Bangladesh", "India", "Iran, Islamic Republic of", "Maldives", "Nepal", "Pakistan", "Sri Lanka"],
    "Eastern Asia": ["CN", "China", "Hong Kong", "Japan", "Korea, Republic of", "Macao", "Taiwan, Province of China"],
    "South-eastern Asia": ["Cambodia", "East Timor", "Indonesia", "Lao People's Democratic Republic", "Malaysia", "Myanmar", "Philippines", "Singapore", "Thailand", "Viet Nam"],
    "Central Asia": ["Kazakhstan", "Tajikistan", "Uzbekistan"],
    "Northern Africa": ["Algeria", "Egypt", "Libya", "Morocco", "Sudan", "Tunisia"],
    "Western Africa": ["Benin", "Burkina Faso", "Cabo Verde", "Cameroon", "Côte d'Ivoire", "Ghana", "Guinea-Bissau", "Mali", "Nigeria", "Senegal", "Sierra Leone", "Togo"],
    "Middle Africa": ["Angola", "Central African Republic", "Gabon", "Sao Tome and Principe"],
    "Eastern Africa": ["Burundi", "Djibouti", "Ethiopia", "Kenya", "Madagascar", "Malawi", "Mauritius", "Mayotte", "Mozambique", "Rwanda", "Seychelles", "Tanzania, United Republic of", "Uganda", "Zambia", "Zimbabwe"],
    "Southern Africa": ["Botswana", "Namibia", "South Africa"],
    "Sub-Saharan Africa": ["French Southern Territories"],
    "Caribbean": ["Anguilla", "Aruba", "Bahamas", "Barbados", "Cayman Islands", "Cuba", "Dominica", "Dominican Republic", "Guadeloupe", "Jamaica", "Puerto Rico", "Saint Kitts and Nevis"],
    "Central America": ["Costa Rica", "El Salvador", "Guatemala", "Honduras", "Mexico", "Panama"],
    "South America": ["Argentina", "Bolivia, Plurinational State of", "Brazil", "Chile", "Colombia", "Ecuador", "Guyana", "Paraguay", "Peru", "Suriname", "Uruguay", "Venezuela, Bolivarian Republic of"],
    "Northern America": ["United States"],
    "Australia and New Zealand": ["Australia", "New Zealand"],
    "Melanesia": ["Fiji", "New Caledonia"],
    "Polynesia": ["American Samoa", "French Polynesia"],
    "Micronesia": ["Kiribati", "Palau", "United States Minor Outlying Islands"],
    "Antarctica": ["Antarctica"],
}

region_continent = {
    "Southern Europe": "Europe", "Northern Europe": "Europe",
    "Western Europe": "Europe", "Eastern Europe": "Europe",
    "Western Asia": "Asia", "Southern Asia": "Asia",
    "Eastern Asia": "Asia", "South-eastern Asia": "Asia", "Central Asia": "Asia",
    "Northern Africa": "Africa", "Western Africa": "Africa",
    "Middle Africa": "Africa", "Eastern Africa": "Africa",
    "Southern Africa": "Africa", "Sub-Saharan Africa": "Africa",
    "Caribbean": "Americas", "Central America": "Americas",
    "South America": "Americas", "Northern America": "Americas",
    "Australia and New Zealand": "Oceania", "Melanesia": "Oceania",
    "Polynesia": "Oceania", "Micronesia": "Oceania",
    "Antarctica": "Antarctica",
}

country_region = {}
for region, countries in region_countries.items():
    for country in countries:
        country_region[country] = region

df['region'] = df['country'].map(country_region)
df['continent'] = df['region'].map(region_continent)

df.to_csv(r"C:\SRP_projekt-main\checkpoint2\hotel_bookings2.csv", index=False, encoding='utf-8')

print(f"Gotovo! {len(df)} redova zapisano u hotel_bookings2.csv")